//! Reader example.
//!
//! In this usage example a process reserves and deletes jobs from beanstalkd
//!
//! Run in a terminal:
//!
//!     cargo run --example reader
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

    // Choose the beanstalkd tube from which the jobs will be fetched
    let tube_name = std::env::var("BEANSTALKD_TUBE").unwrap_or(String::from("default"));
    beanstalk_client
        .watch_tube(&tube_name)
        .await
        .expect("Watch a tube");

    // Show the watched tubes (they should be "default" and `tube_name`)
    let watched_tubes = beanstalk_client
        .list_tubes_watched()
        .await
        .expect("List watched tubes");
    log::info!("The watched tubes are: {:?}", watched_tubes);

    // Reserve the next job from the watched tube
    let job = beanstalk_client
        .reserve()
        .await
        .expect("Reserve a job from the beanstalkd tube");
    log::info!("Read job with id: '{}' and data: {}", job.id, job.payload);

    // Delete the job from beanstalkd
    beanstalk_client
        .delete(job.id)
        .await
        .expect("Delete a reserved job from beanstalkd");
}
