use futures::future::join_all;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::TopicPartitionList;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Metrics {
    name: String,
    latencies: Vec<u128>,
    p999: f64,
    p99: f64,
    p95: f64,
    p50: f64,
}

impl Metrics {
    fn calculate_tail_latencies(&mut self) {
        self.latencies.sort();
        self.p99 = calculate_percentile(&self.latencies, 99.0);
        self.p999 = calculate_percentile(&self.latencies, 99.9);
        self.p95 = calculate_percentile(&self.latencies, 95.0);
        self.p50 = calculate_percentile(&self.latencies, 50.0);
    }

    fn print_results(&self) {
        println!("{} Latency Percentiles:", self.name);
        print!("p50: {:.2}ms, ", self.p50);
        print!("p95: {:.2}ms, ", self.p95);
        print!("p99: {:.2}ms, ", self.p99);
        print!("p999: {:.2}ms", self.p999);
        println!();
    }
}

fn calculate_percentile(sorted_data: &[u128], percentile: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }

    let rank = percentile / 100.0 * (sorted_data.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;

    if upper >= sorted_data.len() {
        return sorted_data[sorted_data.len() - 1] as f64;
    }

    let weight = rank - lower as f64;
    sorted_data[lower] as f64 * (1.0 - weight) + sorted_data[upper] as f64 * weight
}

#[tokio::main]
async fn main() {
    let brokers = "localhost:19092";

    let mut handles = Vec::with_capacity(10);
    for i in 0..4 {
        let handle = tokio::task::spawn(async move {
            let producer: BaseProducer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("linger.ms", "10")
                .set("batch.num.messages", "1024")
                .create()
                .expect("Producer creation error");

            let cg_id = format!("my_cg_{}", i);
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", cg_id)
                .set("bootstrap.servers", brokers)
                .set("enable.auto.commit", "true")
                .set("fetch.min.bytes", "1048576")
                .set("fetch.wait.max.ms", "100")
                .set("auto.offset.reset", "earliest")
                .create()
                .expect("Consumer creation failed");
            let mut tpl = TopicPartitionList::new();
            let le_topic = format!("my_topic_{}", i);

            let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .create()
                .expect("Failed to create Kafka admin client");

            let new_topic = NewTopic::new(&le_topic, 1, TopicReplication::Fixed(1))
                .set("segment.bytes", "1073741824");

            match admin
                .create_topics(&[new_topic], &AdminOptions::new())
                .await {
                    Ok(result) => {
                        for res in result {
                            if let Ok(r) = res {
                                println!("topic created: {}", r);
                            }
                        }

                    }
                    Err(_) => {}
                }

            tpl.add_partition(&le_topic, 0);
            consumer.assign(&tpl).expect("Failed to assign partition");

            let mut metrics = Metrics {
                name: format!("Actor numero {}", i),
                latencies: Vec::new(),
                p999: 0.0,
                p99: 0.0,
                p95: 0.0,
                p50: 0.0,
            };

            let message_size = 1024; // 1 KB
            let payload = "a".repeat(message_size);
            for i in 0..1000000 {
                let key = format!("key-{}", i);
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                let record = BaseRecord::to(&le_topic)
                    .partition(0)
                    .timestamp(timestamp as i64)
                    .payload(&payload)
                    .key(&key);

                if let Err((e, _)) = producer.send(record) {
                    eprintln!("Failed to send message {}: {:?}", i, e);
                }

                if i % 1024 == 0 && i != 0 {
                    producer.flush(Duration::from_secs(100)).unwrap();
                    let mut consumed = 0;
                    loop {
                        match consumer.recv().await {
                            Err(e) => eprintln!("Error receiving message: {:?}", e),
                            Ok(m) => {
                                //if consumed == 0 {
                                    let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis()
                                        as i64;
                                    let timestamp = m.timestamp().to_millis().unwrap();
                                    let latency = (now - timestamp) as u128;
                                    metrics.latencies.push(latency);
                                //}
                                consumed += 1;
                            }
                        }
                        if consumed == 1024 {
                            break;
                        }
                    }
                }
            }
            metrics.calculate_tail_latencies();
            metrics.print_results();
        });
        handles.push(handle);
    }
    join_all(handles).await;
}
