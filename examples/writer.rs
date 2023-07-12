//! Writer example.
//!
//! In this usage example a process is adding jobs to beanstalkd
//!
//! Run in a terminal:
//!
//!     cargo run --example writer
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

    // Choose the beanstalkd tube where the jobs will be added
    let tube = std::env::var("BEANSTALKD_TUBE").unwrap_or(String::from("default"));
    beanstalk_client.use_tube(&tube).await.expect("Use a tube");

    // Add a job to the tube
    beanstalk_client
        .put(String::from("any-job-data"))
        .await
        .expect("Add a job to the beanstalkd tube");
}
