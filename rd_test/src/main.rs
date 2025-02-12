use std::{sync::Arc, time::Duration};

use admin::Admin;
use futures::{
    future::{select_all, try_join_all},
    stream, StreamExt,
};
use rdkafka::{
    producer::{BaseProducer, BaseRecord, FutureProducer, FutureRecord, Producer, ThreadedProducer},
    util::Timeout,
    ClientConfig,
};
use tokio::time::Instant;
pub mod admin;
use std::thread;

#[tokio::main]
async fn main() {
    let brokers = "localhost:19092";
    let base_topic = "test";
    let total_messages = 8_000_000;
    let num_producers = 8;
    let messages_per_producer = total_messages / num_producers;

    let mut handles = vec![];

    let mut topics = Vec::new();
    for id in 0..num_producers {
        let topic = format!("{}-{}", base_topic, id);
        topics.push(topic);
    }
    let admin = Admin::new(&brokers);
    admin.create_topics(&topics, 1).await.unwrap();
    let topics = Arc::new(topics);

    for thread_id in 0..num_producers {
        let brokers = brokers.to_string();
        let topics = topics.clone();
        let handle = thread::spawn(move || {
            // Create thread-specific topic name

            // Create producer with dedicated topic
            let producer: BaseProducer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("linger.ms", "1")
                .set("batch.num.messages", "1000")
                .set("batch.size", "20485760")
                .set("message.max.bytes", "1048588")
                .set("max.in.flight", "1")
                .create()
                .expect("Producer creation failed");

            let payload = vec![0u8; 1024];
            let st = String::from_utf8(payload).unwrap();
            let mut stats = LocalStats::new(thread_id);

            for i in 1..=messages_per_producer {
                let record = BaseRecord::to(&topics[thread_id]).key("test").payload(&st);

                if let Err((e, _)) = producer.send(record) {
                    stats.log_error();
                    eprintln!("[Thread {}] Error: {}", thread_id, e);
                }

                if i % 1000 == 0 {
                    producer.flush(Duration::from_secs(10)).unwrap();
                    stats.record_batch();
                }
            }

            stats.print_final();
            stats
        });
        handles.push(handle);
    }

    // Collect and aggregate final statistics
    let mut all_latencies = Vec::new();
    let mut total_errors = 0;

    for handle in handles {
        if let Ok(thread_stats) = handle.join() {
            all_latencies.extend(thread_stats.latencies);
            total_errors += thread_stats.error_count;
        }
    }

    // Calculate global percentiles
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
        let elapsed = self.batch_time.elapsed().as_millis() as f64;
        self.latencies.push(elapsed);
        self.batch_count += 1;
        self.batch_time = Instant::now();
    }

    fn log_error(&mut self) {
        self.error_count += 1;
    }

    fn print(&self) {
        let avg = self.latencies.iter().sum::<f64>() / self.latencies.len() as f64;
        println!(
            "[Thread {}] Batches: {} | Avg: {:.2}ms | Errors: {}",
            self.thread_id, self.batch_count, avg, self.error_count
        );
    }

    fn print_final(&self) {
        let total_time = self.start_time.elapsed().as_secs_f64();
        let throughput = (self.batch_count * 1000) as f64 / total_time;
        println!(
            "[Thread {}] Final: {:.0} msg/s | Total Errors: {}",
            self.thread_id, throughput, self.error_count
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
