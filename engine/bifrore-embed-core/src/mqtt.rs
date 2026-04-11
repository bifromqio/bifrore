use crate::message::Message;
use std::sync::Arc;
#[cfg(feature = "mqtt")]
use std::thread::JoinHandle;
#[cfg(feature = "mqtt")]
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_MAX_CLIENTS_PER_NCI: u32 = 55_000;

#[derive(Clone)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub node_id: String,
    pub client_count: u16,
    pub client_ids: Vec<String>,
    pub io_threads: u16,
    pub eval_threads: u16,
    pub queue_capacity: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub clean_start: bool,
    pub session_expiry_interval: u32,
    pub group_name: String,
    pub ordered: bool,
    pub ordered_prefix: String,
    pub keep_alive_secs: u16,
    pub multi_nci: bool,
}

impl MqttConfig {
    pub fn shared_subscription(&self, topic_filter: &str) -> String {
        if self.ordered {
            format!("{}/{}/{}", self.ordered_prefix, self.group_name, topic_filter)
        } else {
            format!("$share/{}/{}", self.group_name, topic_filter)
        }
    }

    pub fn client_id_for(&self, index: u16) -> String {
        self.client_ids.get(index as usize).unwrap().clone()
    }
}

pub struct IncomingDelivery {
    pub message: Message,
    #[cfg(feature = "mqtt")]
    ack: Option<DeferredAck>,
}

impl IncomingDelivery {
    #[cfg(feature = "mqtt")]
    pub fn ack(self) {
        if let Some(ack) = self.ack {
            if let Err(err) = ack.client.try_ack(&ack.publish) {
                log::warn!("failed to send MQTT PUBACK/PUBREC after eval: {err}");
            }
        }
    }

    #[cfg(not(feature = "mqtt"))]
    pub fn ack(self) {
        let _ = self;
    }
}

pub type MessageHandler = Arc<dyn Fn(IncomingDelivery) + Send + Sync + 'static>;

#[derive(Debug)]
pub enum MqttError {
    Disabled,
    StartFailed(String),
}

pub struct MqttAdapterHandle {
    #[cfg(feature = "mqtt")]
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    #[cfg(feature = "mqtt")]
    runtime_thread: Option<JoinHandle<()>>,
}

#[cfg(feature = "mqtt")]
struct DeferredAck {
    client: rumqttc::v5::AsyncClient,
    publish: rumqttc::v5::mqttbytes::v5::Publish,
}

impl MqttAdapterHandle {
    #[allow(unused_mut)]
    pub fn stop(mut self) -> Result<(), MqttError> {
        #[cfg(feature = "mqtt")]
        {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            if let Some(runtime_thread) = self.runtime_thread.take() {
                let _ = runtime_thread.join();
            }
            Ok(())
        }
        #[cfg(not(feature = "mqtt"))]
        {
            Err(MqttError::Disabled)
        }
    }
}

#[cfg(feature = "mqtt")]
pub fn start_mqtt(
    config: MqttConfig,
    topics: Vec<String>,
    handler: MessageHandler,
) -> Result<MqttAdapterHandle, MqttError> {
    use rumqttc::v5::{AsyncClient, MqttOptions};
    use std::sync::mpsc;
    use std::time::Duration;

    let client_count = config.client_count.max(1);
    if config.client_ids.len() != client_count as usize {
        return Err(MqttError::StartFailed(format!(
            "invalid client_ids length: expected {}, got {}",
            client_count,
            config.client_ids.len()
        )));
    }
    let io_threads = config.io_threads.max(1) as usize;

    let multi_nci_devices = select_multi_nci_devices(config.multi_nci, client_count);
    log::info!(
        "starting MQTT adapter host={} port={} topics={} clients={} io_threads={} queue_capacity={} multi_nci={} devices={}",
        config.host,
        config.port,
        topics.len(),
        client_count,
        io_threads,
        config.queue_capacity.max(1),
        config.multi_nci,
        multi_nci_devices.as_ref().map(|v| v.len()).unwrap_or(0)
    );

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
    let handler_runtime = handler.clone();
    let runtime_thread = std::thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(io_threads)
            .thread_name("bifrore-mqtt-io")
            .build()
        {
            Ok(runtime) => runtime,
            Err(err) => {
                log::error!("failed to create shared tokio runtime: {err}");
                let _ = ready_tx.send(Err(format!("failed to create runtime: {err}")));
                return;
            }
        };

        runtime.block_on(async move {
            let (shutdown_tx, _) = tokio::sync::watch::channel(false);
            let mut tasks = Vec::with_capacity(client_count as usize);
            let mut subscribed_clients = 0usize;

            for index in 0..client_count {
                let client_id = config.client_id_for(index);
                let mut mqtt_options =
                    MqttOptions::new(client_id.clone(), config.host.clone(), config.port);
                mqtt_options.set_keep_alive(Duration::from_secs(config.keep_alive_secs.into()));
                mqtt_options.set_clean_start(config.clean_start);
                mqtt_options.set_manual_acks(true);
                let bind_device = multi_nci_devices
                    .as_ref()
                    .and_then(|devices| devices.get((index as usize) % devices.len()))
                    .map(|value| value.as_str());
                apply_bind_device(&mut mqtt_options, bind_device, &client_id);
                let mut connect_properties = rumqttc::v5::mqttbytes::v5::ConnectProperties::new();
                connect_properties.session_expiry_interval = Some(config.session_expiry_interval);
                mqtt_options.set_connect_properties(connect_properties);
                if let Some(username) = config.username.as_ref() {
                    mqtt_options.set_credentials(
                        username.clone(),
                        config.password.clone().unwrap_or_default(),
                    );
                }

                let (client, event_loop) = AsyncClient::new(mqtt_options, 10);
                if let Err(err) = subscribe_topics(&client, &config, &topics).await {
                    log::error!(
                        "failed to subscribe MQTT topics for client_id={client_id}: {err:?}"
                    );
                    continue;
                }

                log::info!("MQTT subscriptions established for client_id={client_id}");
                subscribed_clients += 1;
                let handler = handler_runtime.clone();
                let shutdown_rx = shutdown_tx.subscribe();
                tasks.push(tokio::spawn(run_event_loop(
                    client.clone(),
                    event_loop,
                    handler,
                    shutdown_rx,
                )));
            }

            if subscribed_clients == 0 {
                let _ = ready_tx.send(Err("no MQTT clients subscribed".to_string()));
            } else {
                let _ = ready_tx.send(Ok(()));
            }

            let _ = stop_rx.await;
            let _ = shutdown_tx.send(true);
            for task in tasks {
                let _ = task.await;
            }
        });
    });

    match ready_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            log::error!("MQTT startup failed: {err}");
            let _ = stop_tx.send(());
            let _ = runtime_thread.join();
            return Err(MqttError::StartFailed(err));
        }
        Err(_) => {
            let err = "timeout waiting MQTT startup readiness".to_string();
            log::error!("{err}");
            let _ = stop_tx.send(());
            let _ = runtime_thread.join();
            return Err(MqttError::StartFailed(err));
        }
    }

    Ok(MqttAdapterHandle {
        stop: Some(stop_tx),
        runtime_thread: Some(runtime_thread),
    })
}

#[cfg(feature = "mqtt")]
fn select_multi_nci_devices(multi_nci: bool, client_count: u16) -> Option<Vec<String>> {
    if !multi_nci {
        return None;
    }
    let required_nci = (client_count.max(1) as u32)
        .div_ceil(DEFAULT_MAX_CLIENTS_PER_NCI)
        .max(1);
    if required_nci <= 1 {
        return None;
    }
    let devices = detect_provisioned_ncis();
    if devices.len() < required_nci as usize {
        log::warn!(
            "multi_nci enabled but detected interfaces={} required={}; fallback to default interface",
            devices.len(),
            required_nci
        );
        return None;
    }
    let selected = devices.into_iter().take(required_nci as usize).collect::<Vec<_>>();
    log::info!(
        "multi_nci enabled with {} provisioned interfaces: {:?}",
        selected.len(),
        selected
    );
    Some(selected)
}

#[cfg(feature = "mqtt")]
fn detect_provisioned_ncis() -> Vec<String> {
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    {
        let mut interfaces = std::fs::read_dir("/sys/class/net")
            .ok()
            .into_iter()
            .flat_map(|entries| entries.flatten())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name != "lo")
            .collect::<Vec<_>>();
        interfaces.sort();
        interfaces
    }
    #[cfg(not(any(target_os = "android", target_os = "fuchsia", target_os = "linux")))]
    {
        Vec::new()
    }
}

#[cfg(feature = "mqtt")]
fn apply_bind_device(
    mqtt_options: &mut rumqttc::v5::MqttOptions,
    bind_device: Option<&str>,
    client_id: &str,
) {
    if let Some(bind_device) = bind_device {
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        {
            let mut network_options = mqtt_options.network_options();
            network_options.set_bind_device(bind_device);
            mqtt_options.set_network_options(network_options);
            log::info!(
                "MQTT client_id={} bound to network device={}",
                client_id,
                bind_device
            );
        }
        #[cfg(not(any(target_os = "android", target_os = "fuchsia", target_os = "linux")))]
        {
            let _ = mqtt_options;
            log::warn!(
                "bind_device={} configured for client_id={} but this platform does not support bind_device",
                bind_device,
                client_id
            );
        }
    }
}

#[cfg(feature = "mqtt")]
async fn subscribe_topics(
    client: &rumqttc::v5::AsyncClient,
    config: &MqttConfig,
    topics: &[String],
) -> Result<(), MqttError> {
    for topic in topics {
        let shared = config.shared_subscription(topic);
        log::debug!("subscribing MQTT topic filter={} shared={}", topic, shared);
        client
            .subscribe(shared, rumqttc::v5::mqttbytes::QoS::AtLeastOnce)
            .await
            .map_err(|err| MqttError::StartFailed(err.to_string()))?;
    }
    Ok(())
}

#[cfg(feature = "mqtt")]
async fn run_event_loop(
    client: rumqttc::v5::AsyncClient,
    mut event_loop: rumqttc::v5::EventLoop,
    handler: MessageHandler,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    break;
                }
            }
            event = event_loop.poll() => {
                match event {
                    Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(publish))) => {
                        let topic = String::from_utf8_lossy(&publish.topic).into_owned();
                        let mut msg = Message::new(topic, publish.payload.to_vec());
                        log::trace!("Received publish packet: {:?}", msg);
                        msg.qos = publish.qos as u8;
                        msg.retain = publish.retain;
                        msg.dup = publish.dup;
                        msg.timestamp_millis = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        if let Some(properties) = publish.properties.as_ref() {
                            if let Some(content_type) = properties.content_type.as_ref() {
                                msg.properties
                                    .insert("content-type".to_string(), content_type.clone());
                            }
                            for (key, value) in properties.user_properties.iter() {
                                msg.properties.insert(key.clone(), value.clone());
                            }
                        }
                        let task = IncomingDelivery {
                            message: msg,
                            ack: Some(DeferredAck {
                                client: client.clone(),
                                publish,
                            }),
                        };
                        handler(task);
                    }
                    Ok(_) => {}
                    Err(err) => {
                        log::error!("MQTT event loop error: {err}");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(not(feature = "mqtt"))]
pub fn start_mqtt(
    _config: MqttConfig,
    _topics: Vec<String>,
    _handler: MessageHandler,
) -> Result<MqttAdapterHandle, MqttError> {
    Err(MqttError::Disabled)
}
