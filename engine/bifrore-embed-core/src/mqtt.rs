use crate::message::Message;
use std::sync::Arc;
#[cfg(feature = "mqtt")]
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub client_prefix: String,
    pub node_id: String,
    pub client_count: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub clean_start: bool,
    pub session_expiry_interval: u32,
    pub group_name: String,
    pub ordered: bool,
    pub ordered_prefix: String,
    pub keep_alive_secs: u16,
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

#[derive(Debug)]
pub enum MqttError {
    Disabled,
    StartFailed(String),
}

pub struct MqttAdapterHandle {
    #[cfg(feature = "mqtt")]
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
}

impl MqttAdapterHandle {
    pub fn stop(self) -> Result<(), MqttError> {
        #[cfg(feature = "mqtt")]
        {
            for stop in self.stops {
                let _ = stop.send(());
            }
            return Ok(());
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

    log::info!(
        "Starting MQTT adapter host={} port={} topics={} clients={}",
        config.host,
        config.port,
        topics.len(),
        config.client_count
    );

    let client_count = if config.client_count == 0 {
        1
    } else {
        config.client_count
    };
    let mut stops = Vec::with_capacity(client_count as usize);
    for index in 0..client_count {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        stops.push(stop_tx);
        let config = config.clone();
        let topics = topics.clone();
        let handler = handler.clone();

        std::thread::spawn(move || {
            let runtime = match tokio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(err) => {
                    log::error!("Failed to create tokio runtime for client index={index}: {err}");
                    return;
                }
            };

            runtime.block_on(async move {
                let client_id = config.client_id_for(index);
                let mut mqtt_options =
                    MqttOptions::new(client_id.clone(), config.host.clone(), config.port);
                mqtt_options.set_keep_alive(Duration::from_secs(config.keep_alive_secs.into()));
                mqtt_options.set_clean_start(config.clean_start);
                let mut connect_properties = rumqttc::v5::mqttbytes::v5::ConnectProperties::new();
                connect_properties.session_expiry_interval = Some(config.session_expiry_interval);
                mqtt_options.set_connect_properties(connect_properties);
                if let Some(username) = config.username.as_ref() {
                    mqtt_options.set_credentials(
                        username.clone(),
                        config.password.clone().unwrap_or_default(),
                    );
                }

                let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);
                if let Err(err) = subscribe_topics(&client, &config, &topics).await {
                    log::error!(
                        "failed to subscribe MQTT topics for client_id={client_id}: {err:?}"
                    );
                    return;
                }

                log::info!("MQTT subscriptions established for client_id={client_id}");
                run_event_loop(&mut event_loop, handler, stop_rx).await;
            });
        });
    }

    Ok(MqttAdapterHandle { stops })
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
    event_loop: &mut rumqttc::v5::EventLoop,
    handler: MessageHandler,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                break;
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
                        if let Some(properties) = publish.properties {
                            if let Some(content_type) = properties.content_type {
                                msg.properties
                                    .insert("content-type".to_string(), content_type);
                            }
                            for (key, value) in properties.user_properties {
                                msg.properties.insert(key, value);
                            }
                        }
                        handler(msg);
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
