use fastrand;
use mamenoki::*;
use regex::Regex;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn use_tube_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = random_testing_tube_name();
        let result = beanstalk_client.use_tube(&tube_name).await.unwrap();
        assert_eq!(format!("USING {}\r\n", tube_name), result);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_tube_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = random_testing_tube_name();
        let result = beanstalk_client.watch_tube(&tube_name).await.unwrap();
        let regex = Regex::new(r"WATCHING \d+\r\n").unwrap();
        assert!(regex.is_match(&result));
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignore_tube_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = random_testing_tube_name();

        let result = beanstalk_client.watch_tube(&tube_name).await.unwrap();
        // The watched tubes are "default" and `tube_name`
        assert_eq!("WATCHING 2\r\n", result);

        let result = beanstalk_client.ignore_tube(&tube_name).await.unwrap();
        assert_eq!("WATCHING 1\r\n", result);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignore_tube_failure_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        // At least a watched tube is required by beanstalkd
        match beanstalk_client.ignore_tube("default").await {
            Ok(_) => panic!("It wasn't expected to succeed"),
            Err(BeanstalkError::UnexpectedResponse(command, response)) => {
                assert_eq!("ignore", command);
                assert_eq!("NOT_IGNORED\r\n", response);
            }
            Err(_) => panic!("It was expected to fail only with a ReservationTimeout"),
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        let result = beanstalk_client
            .put(String::from("job-web-event"))
            .await
            .unwrap();

        // expect that containing INSERTED followed by the id of the created job
        let regex = Regex::new(r"INSERTED \d+\r\n").unwrap();
        assert!(regex.is_match(&result));
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_with_config_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        let priority = 50;
        let delay = 3600;
        let time_to_run = 300;
        let config = PutCommandConfig {
            priority,
            delay,
            time_to_run,
        };
        let result = beanstalk_client
            .put_with_config(String::from("job-web-event"), config)
            .await
            .unwrap();

        let job_id = job_id_from_put_result(&result);
        let job_stats = beanstalk_client.stats_job(job_id).await.unwrap();
        assert_eq!(priority, job_stats.priority);
        assert_eq!(delay, job_stats.delay);
        assert_eq!(time_to_run, job_stats.time_to_run);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserve_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("job-web-event-42"))
            .await
            .unwrap();
        let job = beanstalk_client.reserve().await.unwrap();

        assert_eq!("job-web-event-42", job.payload);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserve_with_timeout_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("job-info"))
            .await
            .unwrap();
        let job = beanstalk_client.reserve_with_timeout(1).await.unwrap();

        assert_eq!("job-info", job.payload);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserve_with_timeout_error_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        match beanstalk_client.reserve_with_timeout(0).await {
            Ok(_) => panic!("It wasn't expected to succeed"),
            Err(BeanstalkError::ReservationTimeout) => {}
            Err(_) => panic!("It was expected to fail only with a ReservationTimeout"),
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_with_config_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client.put(String::from("message")).await.unwrap();
        let job = beanstalk_client.reserve().await.unwrap();

        let release_conf = ReleaseCommandConfig {
            delay: 120,
            ..ReleaseCommandConfig::default()
        };
        let res = beanstalk_client
            .release_with_config(job.id, release_conf)
            .await
            .unwrap();

        assert_eq!("RELEASED\r\n", res);

        let job_stats = beanstalk_client.stats_job(job.id).await.unwrap();
        assert_eq!("delayed", job_stats.state);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_with_config_failure_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        let put_result = beanstalk_client.put(String::from("message")).await.unwrap();
        let job_id = job_id_from_put_result(&put_result);

        let release_conf = ReleaseCommandConfig::default();
        match beanstalk_client.release_with_config(job_id, release_conf).await {
            Ok(_) => panic!("release wasn't expected to return an Ok result"),
            Err(e) => match e {
                BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("release", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("release was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client.put(String::from("message")).await.unwrap();
        let job = beanstalk_client.reserve().await.unwrap();

        let res = beanstalk_client.release(job.id).await.unwrap();

        assert_eq!("RELEASED\r\n", res);

        let job_stats = beanstalk_client.stats_job(job.id).await.unwrap();
        assert_eq!("ready", job_stats.state);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        let put_result = beanstalk_client.put(String::from("a-job")).await.unwrap();
        let job_id = job_id_from_put_result(&put_result);

        let delete_result = beanstalk_client.delete(job_id).await.unwrap();
        assert_eq!("DELETED\r\n", delete_result);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async move {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("first-job"))
            .await
            .unwrap();
        beanstalk_client
            .put(String::from("second-job"))
            .await
            .unwrap();
        beanstalk_client.reserve().await.unwrap();

        let result: Statistics = beanstalk_client.stats().await.unwrap();
        assert!(result.jobs_ready >= 1);
        assert!(result.jobs_reserved >= 1);
        assert!(result.total_jobs >= 1);
        assert!(result.current_connections >= 1);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_job_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_client).await;

        let put_result = beanstalk_client.put(String::from("a-job")).await.unwrap();
        let job_id = job_id_from_put_result(&put_result);

        let job_stats = beanstalk_client.stats_job(job_id).await.unwrap();
        assert_eq!(job_id, job_stats.id);
        assert_eq!(tube_name, job_stats.tube);
        assert_eq!("ready", job_stats.state);
        assert_eq!(DEFAULT_PRIORITY, job_stats.priority);
        assert_eq!(PUT_DEFAULT_DELAY, job_stats.delay);
        assert_eq!(DEFAULT_TIME_TO_RUN, job_stats.time_to_run);
        assert_eq!(0, job_stats.time_left);
        assert_eq!(0, job_stats.file);
        assert_eq!(0, job_stats.reserves);
        assert_eq!(0, job_stats.timeouts);
        assert_eq!(0, job_stats.releases);
        assert_eq!(0, job_stats.buries);
        assert_eq!(0, job_stats.kicks);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_tube_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client.put(String::from("a-job")).await.unwrap();

        let tube_stats = beanstalk_client.stats_tube(&tube_name).await.unwrap();
        assert_eq!(tube_name, tube_stats.name);
        assert_eq!(0, tube_stats.jobs_urgent);
        assert_eq!(1, tube_stats.jobs_ready);
        assert_eq!(0, tube_stats.jobs_reserved);
        assert_eq!(0, tube_stats.jobs_delayed);
        assert_eq!(0, tube_stats.jobs_buried);

        assert_eq!(1, tube_stats.total_jobs);
        assert_eq!(1, tube_stats.using);
        assert_eq!(0, tube_stats.waiting);
        assert_eq!(1, tube_stats.watching);
        assert_eq!(0, tube_stats.pause);
        assert_eq!(0, tube_stats.cmd_delete);
        assert_eq!(0, tube_stats.cmd_pause_tube);
        assert_eq!(0, tube_stats.pause_time_left);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_tube_failure_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        match beanstalk_client.stats_tube(&random_testing_tube_name()).await {
            Ok(_) => panic!("stats_tube wasn't expected to return an Ok result"),
            Err(e) => match e {
                BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("stats-tube", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("stats_tube was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_tubes_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client.put(String::from("a-job")).await.unwrap();

        let tube_stats = beanstalk_client.list_tubes().await.unwrap();
        assert!(tube_stats.contains(&tube_name));
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_tubes_watched_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_client).await;

        let tube_stats = beanstalk_client.list_tubes_watched().await.unwrap();

        assert!(tube_stats.contains(&tube_name));
        assert!(tube_stats.contains(&String::from("default")));
        assert_eq!(2, tube_stats.len());
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_tube_used_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_client).await;

        let used_tube = beanstalk_client.list_tube_used().await.unwrap();

        assert_eq!(tube_name, used_tube);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bury_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("job-blah-blah"))
            .await
            .unwrap();
        let job = beanstalk_client.reserve().await.unwrap();

        let bury_result = beanstalk_client.bury(job.id).await.unwrap();
        assert_eq!("BURIED\r\n", bury_result);

        let job_stats = beanstalk_client.stats_job(job.id).await.unwrap();
        assert_eq!("buried", job_stats.state);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bury_failure_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        match beanstalk_client.bury(789789789).await {
            Ok(_) => panic!("bury wasn't expected to return an Ok value"),
            Err(e) => match e {
                BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("bury", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("bury was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn touch_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("job-blah-blah"))
            .await
            .unwrap();
        let job = beanstalk_client.reserve().await.unwrap();

        let bury_result = beanstalk_client.touch(job.id).await.unwrap();
        assert_eq!("TOUCHED\r\n", bury_result);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn touch_failure_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        match beanstalk_client.touch(123123123123).await {
            Ok(_) => panic!("touch wasn't expected to return an Ok value"),
            Err(e) => match e {
                BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("touch", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("touch was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        let job_result = beanstalk_client
            .put(String::from("job-data"))
            .await
            .unwrap();
        let job_id = job_id_from_put_result(&job_result);

        let job = beanstalk_client.peek(job_id).await.unwrap();
        assert_eq!(job_id, job.id);
        assert_eq!("job-data", job.payload);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_failure_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        match beanstalk_client.peek(456456456456).await {
            Ok(_) => panic!("peek wasn't expected to return an Ok value"),
            Err(e) => match e {
                BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("peek", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("peek was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_ready_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("the-ready-job"))
            .await
            .unwrap();

        let job = beanstalk_client.peek_ready().await.unwrap().unwrap();
        assert_eq!("the-ready-job", job.payload);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_ready_not_found_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        match beanstalk_client.peek_ready().await.unwrap() {
            Some(_) => panic!("peek_ready was not expected to find any ready job"),
            None => {}
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_delayed_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        let config = PutCommandConfig {
            delay: 600,
            ..PutCommandConfig::default()
        };
        beanstalk_client
            .put(String::from("a-ready-job"))
            .await
            .unwrap();
        beanstalk_client
            .put_with_config(String::from("a-delayed-job"), config)
            .await
            .unwrap();

        let job = beanstalk_client.peek_delayed().await.unwrap().unwrap();
        assert_eq!("a-delayed-job", job.payload);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_buried_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("a-buried-job"))
            .await
            .unwrap();
        beanstalk_client
            .put(String::from("a-ready-job"))
            .await
            .unwrap();

        let job = beanstalk_client.reserve().await.unwrap();
        beanstalk_client.bury(job.id).await.unwrap();

        let job = beanstalk_client.peek_buried().await.unwrap().unwrap();
        assert_eq!("a-buried-job", job.payload);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kick_job_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        let delay = 3600;
        let config = PutCommandConfig {
            delay,
            ..PutCommandConfig::default()
        };
        let result = beanstalk_client
            .put_with_config(String::from("job-data"), config)
            .await
            .unwrap();

        let job_id = job_id_from_put_result(&result);
        let job_stats = beanstalk_client.stats_job(job_id).await.unwrap();
        assert_eq!("delayed", job_stats.state);

        beanstalk_client.kick_job(job_id).await.unwrap();

        let job_stats = beanstalk_client.stats_job(job_id).await.unwrap();
        assert_eq!("ready", job_stats.state);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kick_job_not_found_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        match beanstalk_client.kick_job(999999999).await {
            Ok(_) => panic!("kick_job was not expected to find a job with this id"),
            Err(_) => {}
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kick_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        let delay = 3600;
        let config = PutCommandConfig {
            delay,
            ..PutCommandConfig::default()
        };
        let result = beanstalk_client
            .put_with_config(String::from("job-data"), config)
            .await
            .unwrap();

        let job_id = job_id_from_put_result(&result);
        let job_stats = beanstalk_client.stats_job(job_id).await.unwrap();
        assert_eq!("delayed", job_stats.state);

        let result = beanstalk_client.kick(3).await.unwrap();

        assert_eq!("KICKED 1\r\n", result);
        let job_stats = beanstalk_client.stats_job(job_id).await.unwrap();
        assert_eq!("ready", job_stats.state);
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_tube_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("job-data"))
            .await
            .unwrap();

        beanstalk_client.pause_tube(&tube_name, 60).await.unwrap();

        match beanstalk_client.reserve_with_timeout(0).await {
            Ok(_) => panic!("reserve was expected to fail because all the jobs were delayed by the pause command"),
            Err(BeanstalkError::ReservationTimeout) => { },
            Err(_) => panic!("It was expected to fail only with a ReservationTimeout")
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_tube_failure_case_test() {
    let (beanstalk_channel, beanstalk_client) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_client).await;

        beanstalk_client
            .put(String::from("job-data"))
            .await
            .unwrap();

        match beanstalk_client
            .pause_tube("name-of-a-not-existing-tube", 5)
            .await
        {
            Ok(_) => panic!("pause_tube was expected to fail because the tube does not exist"),
            Err(BeanstalkError::UnexpectedResponse(command, response)) => {
                assert_eq!("pause-tube", command);
                assert_eq!("NOT_FOUND\r\n", response);
            }
            Err(_) => panic!("It was expected to fail only with a UnexpectedResponse"),
        }
    };

    run_testing_code(beanstalk_channel, testing_code).await;
}

#[test]
fn put_command_config_defaults_test() {
    let config = PutCommandConfig::default();
    assert_eq!(1025, config.priority);
    assert_eq!(0, config.delay);
    assert_eq!(60, config.time_to_run);
}

// Helper functions ///////////////////////////////////////////////////////////

async fn setup_client() -> (BeanstalkChannel, BeanstalkClient) {
    let beanstalkd_addr =
        std::env::var("BEANSTALKD_ADDR").unwrap_or(String::from("localhost:11300"));
    let beanstalk_channel = BeanstalkChannel::connect(&beanstalkd_addr).await.unwrap();

    let beanstalk_client = beanstalk_channel.create_client();

    (beanstalk_channel, beanstalk_client)
}

async fn in_new_testing_tube(beanstalk_client: &BeanstalkClient) -> String {
    let tube_name = random_testing_tube_name();

    beanstalk_client.use_tube(&tube_name).await.unwrap();
    beanstalk_client.watch_tube(&tube_name).await.unwrap();

    tube_name
}

fn random_testing_tube_name() -> String {
    format!("test-tube.{}", fastrand::u32(..))
}

async fn run_testing_code(
    mut beanstalk: BeanstalkChannel,
    testing_code: impl core::future::Future<Output = ()>,
) {
    tokio::select! {
        _ = beanstalk.run_channel() => assert!(false, "Client should not end before the testing code"),
        _ = testing_code => {},
    };
}

fn job_id_from_put_result(put_result: &String) -> JobId {
    let extract_id_regex = Regex::new(r"INSERTED (\d+)\r\n").unwrap();
    let caps = extract_id_regex.captures(put_result).unwrap();
    caps.get(1).unwrap().as_str().parse::<JobId>().unwrap()
}
