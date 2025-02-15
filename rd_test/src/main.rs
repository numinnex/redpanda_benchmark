use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use admin::Admin;
use futures::{
    future::{select_all, try_join_all},
    stream, StreamExt,
};
use rdkafka::{
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    producer::{
        BaseProducer, BaseRecord, FutureProducer, FutureRecord, Producer, ThreadedProducer,
    },
    statistics::TopicPartition,
    util::Timeout,
    ClientConfig, Message, Timestamp, TopicPartitionList,
};
use tokio::{task, time::Instant};
pub mod admin;
use std::thread;

#[tokio::main]
async fn main() {
    let brokers = "localhost:19092";
    let base_topic = "test";
    let total_messages = 3_000_000;
    let num_producers = 3;
    let messages_per_producer = total_messages / num_producers;

    let mut topics = Vec::new();
    for id in 0..num_producers {
        topics.push(format!("{}-{}", base_topic, id));
    }

    // Create admin client and topics
    let mut topics = Vec::new();
    for id in 0..num_producers {
        let topic = format!("{}-{}", base_topic, id);
        topics.push(topic);
    }
    let admin = Admin::new(&brokers);
    admin.create_topics(&topics, 1).await.unwrap();
    let topics = Arc::new(topics);

    let mut handles = vec![];
    for thread_id in 0..num_producers {
        let brokers = brokers.to_string();
        let topics = Arc::clone(&topics);
        let handle = std::thread::spawn(move || {
            // Create producer wrapped in Arc for thread-safe sharing
            let producer: Arc<BaseProducer> = Arc::new(
                ClientConfig::new()
                    .set("bootstrap.servers", &brokers)
                    .set("linger.ms", "100")
                    .set("batch.num.messages", "1000")
                    .set("batch.size", "1048576") // 20MB
                    .set("message.max.bytes", "1048588") // ~1MB
                    .set("max.in.flight", "1")
                    .create()
                    .expect("Producer creation failed"),
            );

            // Create consumer
            let consumer: Arc<BaseConsumer> = Arc::new(
                ClientConfig::new()
                    .set("bootstrap.servers", &brokers)
                    .set("auto.offset.reset", "earliest")
                    .set("group.id", format!("manual_consumer_{}", thread_id))
                    .set("enable.auto.commit", "false")
                    .set("fetch.min.bytes", "1048588") // ~1MB
                    .create()
                    .expect("Consumer creation failed"),
            );

            let topic = &topics[thread_id];
            let mut tpl = rdkafka::TopicPartitionList::new();
            tpl.add_partition(topic, 0);
            consumer.assign(&tpl).expect("Assignment failed");

            let payload = vec![0u8; 1024];
            let st = String::from_utf8(payload).unwrap();
            let mut stats = LocalStats::new(thread_id);

            for i in 1..=messages_per_producer {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let record = BaseRecord::to(topic)
                    .key("test")
                    .payload(&st)
                    .timestamp(timestamp);

                if let Err((e, _)) = producer.send(record) {
                    stats.log_error();
                    eprintln!("[Thread {}] Error: {}", thread_id, e);
                }

                if i % 1000 == 0 {
                    // Flush the producer in a blocking task
                    let producer = producer.clone();
                    let consumer = consumer.clone();
                    producer.flush(Duration::from_secs(5)).unwrap();

                    // Consume messages asynchronously
                    let mut processed_count = 0;
                    let mut latency_from_batch_stored = false;
                    while let Some(message) = consumer.poll(Duration::from_millis(1)) {
                        if processed_count == 999 {
                            stats.record_batch();
                            processed_count = 0;
                            latency_from_batch_stored = false;
                            // Commit synchronously
                            consumer
                                .commit_consumer_state(rdkafka::consumer::CommitMode::Async)
                                .unwrap();
                            break;
                        }
                        match message {
                            Ok(msg) => {
                                let timestamp = msg.timestamp();
                                processed_count += 1;
                                if let Some(t) = timestamp.to_millis() {
                                    if !latency_from_batch_stored {
                                        let latency = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis()
                                            as i64
                                            - t;
                                        stats.record_latency(latency as f64);
                                        latency_from_batch_stored = true;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Error consuming message: {}", e);
                            }
                        }
                    }
                }
            }

            stats.print_final();
            stats
        });
        handles.push(handle);
    }

    // Collect and aggregate statistics
    let mut all_latencies = Vec::new();
    let mut total_errors = 0;

    for handle in handles {
        if let Ok(thread_stats) = handle.join() {
            all_latencies.extend(thread_stats.latencies);
            total_errors += thread_stats.error_count;
        }
    }

    all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99 = calculate_percentile(&all_latencies, 99.0);
    let p95 = calculate_percentile(&all_latencies, 95.0);
    let p50 = calculate_percentile(&all_latencies, 50.0);

    println!("\nGlobal Statistics:");
    println!("Total Messages: {}", total_messages);
    println!("Total Errors:   {}", total_errors);
    println!("p99 Latency:    {:.2}ms", p99);
    println!("p95 Latency:    {:.2}ms", p95);
    println!("p50 Latency:    {:.2}ms", p50);
}

// Rest of the LocalStats implementation and calculate_percentile remain the same

struct LocalStats {
    thread_id: usize,
    latencies: Vec<f64>,
    error_count: usize,
    batch_count: usize,
    start_time: Instant,
    batch_time: Instant,
}

impl LocalStats {
    fn new(thread_id: usize) -> Self {
        Self {
            thread_id,
            latencies: Vec::new(),
            error_count: 0,
            batch_count: 0,
            start_time: Instant::now(),
            batch_time: Instant::now(),
        }
    }

    fn record_batch(&mut self) {
        self.batch_count += 1;
        self.batch_time = Instant::now();
    }

    fn record_latency(&mut self, latency: f64) {
        self.latencies.push(latency);
    }

    fn log_error(&mut self) {
        self.error_count += 1;
    }

    fn print_final(&mut self) {
        let total_time = self.start_time.elapsed().as_secs_f64();
        let throughput = (self.batch_count * 1000) as f64 / total_time;
        self.latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p99 = calculate_percentile(&self.latencies, 99.0);
        let p95 = calculate_percentile(&self.latencies, 95.0);
        let p50 = calculate_percentile(&self.latencies, 50.0);
        println!(
            "[Thread {}] Final: {:.0} msg/s | p50: {:.2}ms | p95: {:.2}ms | p99: {:.2}ms | Errors: {}",
            self.thread_id, throughput, p50, p95, p99, self.error_count
        );
    }
}

fn calculate_percentile(data: &[f64], percentile: f64) -> f64 {
    let len = data.len();
    if len == 0 {
        return 0.0;
    }
    let index = (percentile / 100.0 * len as f64) as usize;
    data[index.min(len - 1)]
}
