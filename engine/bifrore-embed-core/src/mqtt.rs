use crate::message::Message;
use std::sync::Arc;
#[cfg(feature = "mqtt")]
use std::thread::JoinHandle;
#[cfg(feature = "mqtt")]
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub client_prefix: String,
    pub node_id: String,
    pub client_count: u16,
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
    pub network_provider: Arc<dyn NetworkProvider>,
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
        format!("{}_{}_{}", self.node_id, self.client_prefix, index)
    }
}

pub type MessageHandler = Arc<dyn Fn(Message) + Send + Sync + 'static>;

#[derive(Debug, Clone, Default)]
pub struct NetworkBinding {
    pub bind_device: Option<String>,
}

pub trait NetworkProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn initialize(&self, config: &MqttConfig) -> Result<Box<dyn NetworkProviderSession>, String>;
}

pub trait NetworkProviderSession: Send {
    fn binding_for_client(&mut self, client_index: u16, client_id: &str) -> NetworkBinding;
    fn shutdown(&mut self) {}
}

#[derive(Debug, Clone, Default)]
pub struct OssNetworkProvider;

impl OssNetworkProvider {
    pub fn new() -> Self {
        Self
    }
}

impl NetworkProvider for OssNetworkProvider {
    fn name(&self) -> &'static str {
        "oss-noop"
    }

    fn initialize(&self, config: &MqttConfig) -> Result<Box<dyn NetworkProviderSession>, String> {
        if config.multi_nci {
            log::info!(
                "multi_nci is enabled but OSS network provider is no-op"
            );
        }
        Ok(Box::new(OssNetworkProviderSession))
    }
}

#[derive(Debug, Default)]
struct OssNetworkProviderSession;

impl NetworkProviderSession for OssNetworkProviderSession {
    fn binding_for_client(&mut self, _client_index: u16, _client_id: &str) -> NetworkBinding {
        NetworkBinding::default()
    }
}

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
    #[cfg(feature = "mqtt")]
    eval_worker: Option<JoinHandle<()>>,
}

#[cfg(feature = "mqtt")]
struct QueuedIncoming {
    message: Message,
    ack: Option<DeferredAck>,
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
            if let Some(eval_worker) = self.eval_worker.take() {
                let _ = eval_worker.join();
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
    use std::time::Duration;

    let client_count = config.client_count.max(1);
    let io_threads = config.io_threads.max(1) as usize;
    let queue_capacity = config.queue_capacity.max(1) as usize;

    log::info!(
        "starting MQTT adapter host={} port={} topics={} clients={} io_threads={} queue_capacity={} multi_nci={} provider={}",
        config.host,
        config.port,
        topics.len(),
        client_count,
        io_threads,
        queue_capacity,
        config.multi_nci,
        config.network_provider.name()
    );

    let (queue_tx, queue_rx) = flume::bounded::<QueuedIncoming>(queue_capacity);
    if config.eval_threads > 1 {
        log::warn!(
            "eval_threads={} requested but adapter uses single consumer (MPSC) for ordering simplicity",
            config.eval_threads
        );
    }
    let eval_worker = {
        let eval_handler = handler.clone();
        std::thread::spawn(move || {
            while let Ok(task) = queue_rx.recv() {
                eval_handler(task.message);
                if let Some(ack) = task.ack {
                    if let Err(err) = ack.client.try_ack(&ack.publish) {
                        log::warn!("failed to send MQTT PUBACK/PUBREC after eval: {err}");
                    }
                }
            }
        })
    };

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
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
                return;
            }
        };

        runtime.block_on(async move {
            let (shutdown_tx, _) = tokio::sync::watch::channel(false);
            let mut tasks = Vec::with_capacity(client_count as usize);
            let mut provider_session = match config.network_provider.initialize(&config) {
                Ok(session) => session,
                Err(err) => {
                    log::error!("failed to initialize network provider: {err}");
                    return;
                }
            };

            for index in 0..client_count {
                let client_id = config.client_id_for(index);
                let mut mqtt_options =
                    MqttOptions::new(client_id.clone(), config.host.clone(), config.port);
                mqtt_options.set_keep_alive(Duration::from_secs(config.keep_alive_secs.into()));
                mqtt_options.set_clean_start(config.clean_start);
                mqtt_options.set_manual_acks(true);
                let binding = provider_session.binding_for_client(index, &client_id);
                apply_binding(&mut mqtt_options, &binding, &client_id);
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
                let tx = queue_tx.clone();
                let shutdown_rx = shutdown_tx.subscribe();
                tasks.push(tokio::spawn(run_event_loop(
                    client.clone(),
                    event_loop,
                    tx,
                    shutdown_rx,
                )));
            }

            let _ = stop_rx.await;
            let _ = shutdown_tx.send(true);
            for task in tasks {
                let _ = task.await;
            }
            provider_session.shutdown();
        });
    });

    Ok(MqttAdapterHandle {
        stop: Some(stop_tx),
        runtime_thread: Some(runtime_thread),
        eval_worker: Some(eval_worker),
    })
}

#[cfg(feature = "mqtt")]
fn apply_binding(
    mqtt_options: &mut rumqttc::v5::MqttOptions,
    binding: &NetworkBinding,
    client_id: &str,
) {
    if let Some(bind_device) = binding.bind_device.as_deref() {
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
    queue_tx: flume::Sender<QueuedIncoming>,
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
                        let task = QueuedIncoming {
                            message: msg,
                            ack: Some(DeferredAck {
                                client: client.clone(),
                                publish,
                            }),
                        };
                        if queue_tx.send_async(task).await.is_err() {
                            break;
                        }
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
