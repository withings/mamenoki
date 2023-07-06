//! connection_and_request example.
//!
//! This is a usage example of the beanstalkclient library
//!
//! Run in a terminal:
//!
//!     cargo run --example connection_and_request


// use tokio::time;
use core::time::Duration;
use std::time::SystemTime;

use beanstalkclient::{BeanstalkChannel, BeanstalkError};
use tracing_subscriber;

const PRODUCER_WAIT_TIME_MILLISEC: u64 = 3500;
const RESERVE_FAILURE_WAIT_TIME_SEC: u64 = 1;
const STATS_WAIT_TIME_SEC: u64 = 15;

#[tokio::main]
pub async fn main() {
    match tracing_subscriber::fmt::try_init() {
        Ok(_) => {},
        Err(e) => {
            eprint!("Failed in initialization of the logger {}", e);
            std::process::exit(1);
        }
    }

    let beanstalkd_addr = String::from("localhost:11300");

    // Creating the connection to add jobs to the beanstalkd
    let mut bstk_writer = match BeanstalkChannel::connect(&beanstalkd_addr).await {
        Ok(b) => {
            log::info!("Connection to beanstalkd for writing created");
            b
        },
        Err(e) => {
            log::error!("failed to connect to Beanstalk: {}", e);
            std::process::exit(1);
        }
    };

    // Creating the connection to get jobs and stats from beanstalkd
    let mut bstk_reader = match BeanstalkChannel::connect(&beanstalkd_addr).await {
        Ok(b) => {
            log::info!("Connection to beanstalkd for reading created");
            b
        },
        Err(e) => {
            log::error!("failed to connect to Beanstalk: {}", e);
            std::process::exit(1);
        }
    };

    let writer_client = bstk_writer.create_client();
    let reader_client = bstk_reader.create_client();

    tokio::join!(
        bstk_reader.run_channel(),
        bstk_writer.run_channel(),
        async {
            /* once the PUT channel is ready (has processed the USE command), start writing jobs */
            match writer_client.use_tube("example_connection_and_request").await {
                Ok(_) => {
                    log::info!("writer ready to add jobs!");
                    loop {
                        let event = format!("job-{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs());
                        match writer_client.put(String::from(event)).await {
                            Ok(reply) => {
                                log::info!("Job written to beanstalkd - reply: {}", reply);
                            },
                            Err(e) => {
                                log::warn!("Could not enqueue job: {}", e);
                            }
                        };
                        tokio::time::sleep(Duration::from_millis(PRODUCER_WAIT_TIME_MILLISEC)).await;
                    }
                },
                Err(e) => {
                    log::error!("failed to use beanstalkd tube on webservice connection: {}", e);
                    std::process::exit(1);
                }
            }
        },
        async {
            /* same for WATCH and the forwarder/worker */
            match reader_client.watch_tube("example_connection_and_request").await {
                Ok(_) => {
                    log::info!("reader ready for jobs!");
                    loop {
                        let job = match reader_client.reserve().await {
                            Ok(j) => j,
                            Err(BeanstalkError::ReservationTimeout) => continue,
                            Err(e) => {
                                log::warn!("failed to reserve job, will try again soon: {}", e);
                                tokio::time::sleep(Duration::from_secs(RESERVE_FAILURE_WAIT_TIME_SEC)).await;
                                continue;
                            }
                        };
                
                        /* Immediately delete the job, whatever happens next */
                        log::debug!("new job from beanstalkd: {}", job.payload);
                        if let Err(e) = reader_client.delete(job.id).await {
                            log::error!("failed to delete job, will process anyway: {}", e);
                        }
                    }
                },
                Err(e) => {
                    log::error!("failed to watch beanstalkd tube on forwarder connection: {}", e);
                    std::process::exit(1);
                }
            }
        },
        async {
            // Get the stats from Beanstalkd and print them
            loop {
                match reader_client.stats().await {
                    Ok(stats) => {
                        log::debug!("beanstalkd stats: jobs_ready: {}, jobs_reserved: {}, jobs_delayed: {}, total_jobs: {}, current_connections: {}", 
                            stats.jobs_ready, stats.jobs_reserved, stats.jobs_delayed, stats.total_jobs, stats.current_connections);
                    },
                    Err(BeanstalkError::ReservationTimeout) => continue,
                    Err(e) => {
                        log::warn!("failed to reserve job, will try again soon: {}", e);
                        
                        continue;
                    }
                };
                
                tokio::time::sleep(Duration::from_secs(STATS_WAIT_TIME_SEC)).await;
            }
        }
    );
    
}

