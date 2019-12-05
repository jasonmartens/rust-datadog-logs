use rdkafka::ClientConfig;

fn main() {
    println!("Hello, world!");
    let context = CustomContext();
    let consumer: ProcessingConsumer = ClientConfig
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("statistics.interval.ms", "0")
        .set("fetch.error.backoff.ms", "1")
        .set("auto.offset.reset", "earliest")
        .set_log_level(dbg!(RDKafkaLogLevel))
        .optionally_set_ssl_from_env()
        .create_with_context(context)
        .expect("Consumer creation failed");
}
