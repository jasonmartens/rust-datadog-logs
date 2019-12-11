extern crate clap;
extern crate futures;
#[macro_use]
extern crate log;
extern crate rand;
extern crate rdkafka;
extern crate tokio;

use clap::{App, Arg};
use futures::{lazy, Stream};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use serde_json::{Error, Value, json};
use tokio::runtime::current_thread;

use crate::example_utils::setup_logger;

mod example_utils;

// Emulates an expensive, synchronous computation.
fn expensive_computation<'a>(dd_api_key: String, msg: OwnedMessage) -> String {
    let url = format!("https://http-intake.logs.datadoghq.com/v1/input/{}?ddtags=env:development", dd_api_key);
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => {
            let parsed: Result<Value, Error> = serde_json::from_str(payload);
            match parsed {
                Ok(parsed_json) => {
                    let message_json = match parsed_json["@timestamp"].as_f64() {
                        Some(timestamp) => {
                            let dd_timestamp = (timestamp * 1000.0) as i64;
                            let mut new_json = parsed_json.clone();
                            new_json["@timestamp"] = json!(dd_timestamp);
                            Some(new_json)
                        }
                        None => {
                            None
                        }
                    };
                    let result = match message_json {
                        Some(message) => {
                            let client = reqwest::Client::new();
                            let res = client.post(&url).body(message.to_string()).send();
                            let x: String = match res {
                                Ok(response) => {
                                    format!("Got response: {}", response.status())
                                }
                                Err(e) => {
                                    format!("Got error from http request: {}", e.to_string())
                                }
                            };
                            x
                        }
                        None => {
                            format!("Failed to convert the json payload to a string")
                        }
                    };
                    result
//                    format!("Payload timestamp is {} for offset {}", parsedJson["@timestamp"], msg.offset())
                }
                Err(err) => format!("Failed to parse json. Error: {}", err)
            }
        }
        Some(Err(_)) => "Message payload is not a string".to_owned(),
        None => "No payload".to_owned(),
    }
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
// Moving each message from one stage of the pipeline to next one is handled by the event loop,
// that runs on a single thread. The expensive CPU-bound computation is handled by the `ThreadPool`,
// without blocking the event loop.
fn run_async_processor(dd_api_key: String, brokers: &str, group_id: &str, input_topic: &str) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[input_topic])
        .expect("Can't subscribe to specified topic");

    // Create the runtime where the expensive computation will be performed.
    let mut thread_pool = tokio::runtime::Builder::new()
        .name_prefix("pool-")
        .core_threads(4)
        .build()
        .unwrap();

    // Use the current thread as IO thread to drive consumer and producer.
    let mut io_thread = current_thread::Runtime::new().unwrap();
//    let io_thread_handle = io_thread.handle();

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer
        .start()
        .filter_map(|result| {
            // Filter out errors
            match result {
                Ok(msg) => Some(msg),
                Err(kafka_error) => {
                    warn!("Error while receiving from Kafka: {:?}", kafka_error);
                    None
                }
            }
        })
        .for_each(move |borrowed_message| {
            // Process each message
            info!("Message received: {}", borrowed_message.offset());
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();
            let api_key = dd_api_key.clone();
            let message_future = lazy(move || {
                // The body of this closure will be executed in the thread pool.
                let computation_result = expensive_computation(api_key, owned_message);
                info!("Computation result: {}", computation_result);
                Ok(())
            });
            thread_pool.spawn(message_future);
            Ok(())
        });

    info!("Starting event loop");
    let _ = io_thread.block_on(stream_processor);
    info!("Stream processing terminated");
}

fn main() {
    let matches = App::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
        .arg(
            Arg::with_name("dd-api-key")
                .short("api")
                .long("api-key")
                .help("Datadog API key")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("input-topic")
                .long("input-topic")
                .help("Input topic")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let dd_api_key = matches.value_of("dd-api-key").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let input_topic = matches.value_of("input-topic").unwrap();

    run_async_processor(dd_api_key.to_owned(), brokers, group_id, input_topic);
}
