//! Stats example.
//!
//! In this usage example a process requesting some stats to beanstalkd
//!
//! Run in a terminal:
//!
//!     cargo run --example stats
//!

use mamenoki::*;

#[tokio::main]
pub async fn main() {
    // Setup a logger
    match tracing_subscriber::fmt::try_init() {
        Ok(_) => {}
        Err(e) => {
            eprint!("Failed in initialization of the logger {}", e);
            std::process::exit(1);
        }
    }

    let beanstalkd_addr =
        std::env::var("BEANSTALKD_ADDR").unwrap_or(String::from("localhost:11300"));

    // Create a connection to to the beanstalkd
    let mut beanstalk_channel = match BeanstalkChannel::connect(&beanstalkd_addr).await {
        Ok(b) => {
            log::info!("Connection to beanstalkd for writing created");
            b
        }
        Err(e) => {
            log::error!("failed to connect to Beanstalk: {}", e);
            std::process::exit(1);
        }
    };

    // Create a BeanstalkClient instance that will send the requests to beanstalkd
    // through the Beanstalk channel (via message passing using a mpsc::channel)
    let beanstalk_client: BeanstalkClient = beanstalk_channel.create_client();

    // Run the channel in another task
    tokio::spawn(async move {
        beanstalk_channel.run_channel().await;
    });

    // Get the global beanstalkd stats
    let stats = beanstalk_client
        .stats()
        .await
        .expect("Get the beanstalkd stats");
    log::info!("Beanstalkd stats: {:?}", stats);

    // Use (and create if not existing) a tube
    let tube = std::env::var("BEANSTALKD_TUBE").unwrap_or(String::from("default"));
    beanstalk_client.use_tube(&tube).await.expect("Use a tube");

    // Get the tube stats
    let stats = beanstalk_client
        .stats_tube(&tube)
        .await
        .expect("Get the tube beanstalkd stats");
    log::info!("Tube stats: {:?}", stats);
}
