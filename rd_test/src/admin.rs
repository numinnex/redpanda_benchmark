use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;
use std::time::Duration;

pub struct Admin {
    client: AdminClient<DefaultClientContext>,
}

impl Admin {
    pub fn new(brokers: &str) -> Self {
        let client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("Admin client creation error");

        Admin { client }
    }

    pub async fn topic_exists(&self, topic: &str) -> KafkaResult<bool> {
        let metadata = self.client.inner().fetch_metadata(None, Timeout::Never)?;
        Ok(metadata.topics().iter().any(|t| t.name() == topic))
    }

    pub async fn create_topic(&self, topic: &str, num_partitions: i32) -> KafkaResult<()> {
        let new_topic = NewTopic::new(topic, num_partitions, TopicReplication::Fixed(1));
        let res = self
            .client
            .create_topics(
                &[new_topic],
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(10)))),
            )
            .await?;

        for result in res {
            match result {
                Ok(_) => println!("Topic {} created successfully", topic),
                Err((err, _)) => eprintln!("Failed to create topic {}: {:?}", topic, err),
            }
        }
        Ok(())
    }

    pub async fn create_topics(&self, topics: &Vec<String>, num_partitions: i32) -> KafkaResult<()> {
        let mut xd = Vec::new();
        for topic in topics {
            let new_topic = NewTopic::new(topic, num_partitions, TopicReplication::Fixed(1));
            let new_topic = new_topic.set("segment.bytes", "1073741824");
            xd.push(new_topic);
        }
        let res = self
            .client
            .create_topics(
                &xd,
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(10)))),
            )
            .await?;
        Ok(())
    }
}
