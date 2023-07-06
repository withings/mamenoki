use beanstalkclient::*;
use regex::Regex;
use fastrand;


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn use_tube_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = random_testing_tube_name();
        let result = beanstalk_proxy.use_tube(&tube_name).await.unwrap();
        assert_eq!(format!("USING {}\r\n", tube_name), result);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_tube_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = random_testing_tube_name();
        let result = beanstalk_proxy.watch_tube(&tube_name).await.unwrap();
        let regex = Regex::new(r"WATCHING \d+\r\n").unwrap();
        assert!(regex.is_match(&result));
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignore_tube_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = random_testing_tube_name();
        
        let result = beanstalk_proxy.watch_tube(&tube_name).await.unwrap();
        // The watched tubes are "default" and `tube_name`
        assert_eq!("WATCHING 2\r\n", result);

        let result = beanstalk_proxy.ignore_tube(&tube_name).await.unwrap();
        assert_eq!("WATCHING 1\r\n", result);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignore_tube_failure_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        // At least a watched tube is required by beanstalkd
        match beanstalk_proxy.ignore_tube("default").await {
            Ok(_) => panic!("It wasn't expected to succeed"),
            Err(beanstalkclient::BeanstalkError::UnexpectedResponse(command, response)) => {
                assert_eq!("ignore", command);
                assert_eq!("NOT_IGNORED\r\n", response);
            },
            Err(_) => panic!("It was expected to fail only with a ReservationTimeout")
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        let result = beanstalk_proxy.put(String::from("job-web-event")).await.unwrap();
    
        // expect that containing INSERTED followed by the id of the created job
        let regex = Regex::new(r"INSERTED \d+\r\n").unwrap();
        assert!(regex.is_match(&result));
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_with_config_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        let priority = 50;
        let delay = 3600;
        let time_to_run = 300;
        let config = PutCommandConfig::new(Some(priority), Some(delay), Some(time_to_run));
        let result = beanstalk_proxy.put_with_config(String::from("job-web-event"), config).await.unwrap();
    
        let job_id = job_id_from_put_result(&result);
        let job_stats = beanstalk_proxy.stats_job(job_id).await.unwrap();
        assert_eq!(priority, job_stats.priority);
        assert_eq!(delay, job_stats.delay);
        assert_eq!(time_to_run, job_stats.time_to_run);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserve_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        beanstalk_proxy.put(String::from("job-web-event-42")).await.unwrap();
        let job = beanstalk_proxy.reserve().await.unwrap();

        assert_eq!("job-web-event-42", job.payload);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserve_with_timeout_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        beanstalk_proxy.put(String::from("job-info")).await.unwrap();
        let job = beanstalk_proxy.reserve_with_timeout(1).await.unwrap();
       
        assert_eq!("job-info", job.payload);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserve_with_timeout_error_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        match beanstalk_proxy.reserve_with_timeout(0).await {
            Ok(_) => panic!("It wasn't expected to succeed"),
            Err(beanstalkclient::BeanstalkError::ReservationTimeout) => {},
            Err(_) => panic!("It was expected to fail only with a ReservationTimeout")
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_with_config_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        beanstalk_proxy.put(String::from("message")).await.unwrap();
        let job = beanstalk_proxy.reserve().await.unwrap();
        
        let release_conf = ReleaseCommandConfig::new(Some(10), Some(120));
        let res = beanstalk_proxy.release_with_config(job.id, release_conf).await.unwrap();

        assert_eq!("RELEASED\r\n", res);

        let job_stats = beanstalk_proxy.stats_job(job.id).await.unwrap();
        assert_eq!("delayed", job_stats.state);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_with_config_failure_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        let put_result = beanstalk_proxy.put(String::from("message")).await.unwrap();
        let job_id = job_id_from_put_result(&put_result);
        
        let release_conf = ReleaseCommandConfig::new(None, None);
        match beanstalk_proxy.release_with_config(job_id, release_conf).await {
            Ok(_) => panic!("release wasn't expected to return an Ok result"),
            Err(e) => match e {
                beanstalkclient::BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("release", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("release was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        beanstalk_proxy.put(String::from("message")).await.unwrap();
        let job = beanstalk_proxy.reserve().await.unwrap();
        
        let res = beanstalk_proxy.release(job.id).await.unwrap();

        assert_eq!("RELEASED\r\n", res);

        let job_stats = beanstalk_proxy.stats_job(job.id).await.unwrap();
        assert_eq!("ready", job_stats.state);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        let put_result = beanstalk_proxy.put(String::from("a-job")).await.unwrap();
        let job_id = job_id_from_put_result(&put_result);

        let delete_result = beanstalk_proxy.delete(job_id).await.unwrap();
        assert_eq!("DELETED\r\n", delete_result);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async move {
        in_new_testing_tube(&beanstalk_proxy).await;

        beanstalk_proxy.put(String::from("first-job")).await.unwrap();
        beanstalk_proxy.put(String::from("second-job")).await.unwrap();
        beanstalk_proxy.reserve().await.unwrap();

        let result: Statistics = beanstalk_proxy.stats().await.unwrap();
        assert!(result.jobs_ready >= 1);
        assert!(result.jobs_reserved >= 1);
        assert!(result.total_jobs >= 1);
        assert!(result.current_connections >= 1);
    };

    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_job_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_proxy).await;

        let put_result = beanstalk_proxy.put(String::from("a-job")).await.unwrap();
        let job_id = job_id_from_put_result(&put_result);

        let job_stats = beanstalk_proxy.stats_job(job_id).await.unwrap();
        assert_eq!(job_id, job_stats.id);
        assert_eq!(tube_name, job_stats.tube);
        assert_eq!("ready", job_stats.state);
        assert_eq!(beanstalkclient::DEFAULT_PRIORITY, job_stats.priority);
        assert_eq!(beanstalkclient::PUT_DEFAULT_DELAY, job_stats.delay);
        assert_eq!(beanstalkclient::DEFAULT_TIME_TO_RUN, job_stats.time_to_run);
        assert_eq!(0, job_stats.time_left);
        assert_eq!(0, job_stats.file);
        assert_eq!(0, job_stats.reserves);
        assert_eq!(0, job_stats.timeouts);
        assert_eq!(0, job_stats.releases);
        assert_eq!(0, job_stats.buries);
        assert_eq!(0, job_stats.kicks);
    };

    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_tube_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_proxy).await;

        beanstalk_proxy.put(String::from("a-job")).await.unwrap();

        let tube_stats = beanstalk_proxy.stats_tube(&tube_name).await.unwrap();
        assert_eq!(tube_name, tube_stats.name);
        assert_eq!(1, tube_stats.jobs_urgent);
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

    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_tube_failure_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        match beanstalk_proxy.stats_tube(&random_testing_tube_name()).await {
            Ok(_) => panic!("stats_tube wasn't expected to return an Ok result"),
            Err(e) => match e {
                beanstalkclient::BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("stats-tube", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("stats_tube was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };

    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_tubes_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_proxy).await;

        beanstalk_proxy.put(String::from("a-job")).await.unwrap();

        let tube_stats = beanstalk_proxy.list_tubes().await.unwrap();
        assert!(tube_stats.contains(&tube_name));
    };

    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_tubes_watched_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_proxy).await;

        let tube_stats = beanstalk_proxy.list_tubes_watched().await.unwrap();
        
        assert!(tube_stats.contains(&tube_name));
        assert!(tube_stats.contains(&String::from("default")));
        assert_eq!(2, tube_stats.len());
    };

    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_tube_used_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_proxy).await;

        let used_tube = beanstalk_proxy.list_tube_used().await.unwrap();
        
        assert_eq!(tube_name, used_tube);
    };

    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bury_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        beanstalk_proxy.put(String::from("job-blah-blah")).await.unwrap();
        let job = beanstalk_proxy.reserve().await.unwrap();

        let bury_result = beanstalk_proxy.bury(job.id).await.unwrap();
        assert_eq!("BURIED\r\n", bury_result);

        let job_stats = beanstalk_proxy.stats_job(job.id).await.unwrap();
        assert_eq!("buried", job_stats.state);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bury_failure_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        match beanstalk_proxy.bury(789789789).await {
            Ok(_) => panic!("bury wasn't expected to return an Ok value"),
            Err(e) => match e {
                beanstalkclient::BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("bury", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("bury was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn touch_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        beanstalk_proxy.put(String::from("job-blah-blah")).await.unwrap();
        let job = beanstalk_proxy.reserve().await.unwrap();

        let bury_result = beanstalk_proxy.touch(job.id).await.unwrap();
        assert_eq!("TOUCHED\r\n", bury_result);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn touch_failure_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        match beanstalk_proxy.touch(123123123123).await {
            Ok(_) => panic!("touch wasn't expected to return an Ok value"),
            Err(e) => match e {
                beanstalkclient::BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("touch", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("touch was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        let job_result = beanstalk_proxy.put(String::from("job-data")).await.unwrap();
        let job_id = job_id_from_put_result(&job_result);

        let job = beanstalk_proxy.peek(job_id).await.unwrap();
        assert_eq!(job_id, job.id);
        assert_eq!("job-data", job.payload);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_failure_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        match beanstalk_proxy.peek(456456456456).await {
            Ok(_) => panic!("peek wasn't expected to return an Ok value"),
            Err(e) => match e {
                beanstalkclient::BeanstalkError::UnexpectedResponse(command, response) => {
                    assert_eq!("peek", command);
                    assert_eq!("NOT_FOUND\r\n", response);
                },
                _ => panic!("peek was expected to return an error of type BeanstalkError::UnexpectedResponse")
            }
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_ready_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        beanstalk_proxy.put(String::from("the-ready-job")).await.unwrap();

        let job = beanstalk_proxy.peek_ready().await.unwrap().unwrap();
        assert_eq!("the-ready-job", job.payload);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_ready_not_found_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        match beanstalk_proxy.peek_ready().await.unwrap() {
            Some(_) => panic!("peek_ready was not expected to find any ready job"),
            None => {}
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_delayed_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        let config = PutCommandConfig::new(None, Some(600), None);
        beanstalk_proxy.put(String::from("a-ready-job")).await.unwrap();
        beanstalk_proxy.put_with_config(String::from("a-delayed-job"), config).await.unwrap();

        let job = beanstalk_proxy.peek_delayed().await.unwrap().unwrap();
        assert_eq!("a-delayed-job", job.payload);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_buried_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        beanstalk_proxy.put(String::from("a-buried-job")).await.unwrap();
        beanstalk_proxy.put(String::from("a-ready-job")).await.unwrap();

        let job = beanstalk_proxy.reserve().await.unwrap();
        beanstalk_proxy.bury(job.id).await.unwrap();

        let job = beanstalk_proxy.peek_buried().await.unwrap().unwrap();
        assert_eq!("a-buried-job", job.payload);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kick_job_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        let delay = 3600;
        let config = PutCommandConfig::new(None, Some(delay), None);
        let result = beanstalk_proxy.put_with_config(String::from("job-data"), config).await.unwrap();
    
        let job_id = job_id_from_put_result(&result);
        let job_stats = beanstalk_proxy.stats_job(job_id).await.unwrap();
        assert_eq!("delayed", job_stats.state);

        beanstalk_proxy.kick_job(job_id).await.unwrap();

        let job_stats = beanstalk_proxy.stats_job(job_id).await.unwrap();
        assert_eq!("ready", job_stats.state);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kick_job_not_found_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        match beanstalk_proxy.kick_job(999999999).await {
            Ok(_) => panic!("kick_job was not expected to find a job with this id"),
            Err(_) => {}
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kick_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        let delay = 3600;
        let config = PutCommandConfig::new(None, Some(delay), None);
        let result = beanstalk_proxy.put_with_config(String::from("job-data"), config).await.unwrap();
    
        let job_id = job_id_from_put_result(&result);
        let job_stats = beanstalk_proxy.stats_job(job_id).await.unwrap();
        assert_eq!("delayed", job_stats.state);

        let result = beanstalk_proxy.kick(3).await.unwrap();
        
        assert_eq!("KICKED 1\r\n", result);
        let job_stats = beanstalk_proxy.stats_job(job_id).await.unwrap();
        assert_eq!("ready", job_stats.state);
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_tube_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        let tube_name = in_new_testing_tube(&beanstalk_proxy).await;

        beanstalk_proxy.put(String::from("job-data")).await.unwrap();

        beanstalk_proxy.pause_tube(&tube_name, 60).await.unwrap();

        match beanstalk_proxy.reserve_with_timeout(0).await {
            Ok(_) => panic!("reserve was expected to fail because all the jobs were delayed by the pause command"),
            Err(beanstalkclient::BeanstalkError::ReservationTimeout) => { },
            Err(_) => panic!("It was expected to fail only with a ReservationTimeout")
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_tube_failure_case_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        beanstalk_proxy.put(String::from("job-data")).await.unwrap();

        match beanstalk_proxy.pause_tube("name-of-a-not-existing-tube", 5).await {
            Ok(_) => panic!("pause_tube was expected to fail because the tube does not exist"),
            Err(beanstalkclient::BeanstalkError::UnexpectedResponse(command, response)) => {
                assert_eq!("pause-tube", command);
                assert_eq!("NOT_FOUND\r\n", response);
            },
            Err(_) => panic!("It was expected to fail only with a UnexpectedResponse")
        }
    };
    
    run_testing_code(beanstalk_client, testing_code).await;
}

#[test]
fn put_command_config_defaults_test() {
    let config = PutCommandConfig::new(None, None, None);
    assert_eq!(0, config.priority);
    assert_eq!(0, config.delay);
    assert_eq!(60, config.time_to_run);
}


// Helper functions ///////////////////////////////////////////////////////////

async fn setup_client() -> (Beanstalk, BeanstalkProxy) {
    let beanstalkd_addr = String::from("localhost:11300");
    let beanstalk_client = Beanstalk::connect(&beanstalkd_addr).await.unwrap();

    let beanstalk_proxy = beanstalk_client.proxy();

    (beanstalk_client, beanstalk_proxy)
}

async fn in_new_testing_tube(beanstalk_proxy: &BeanstalkProxy) -> String {
    let tube_name = random_testing_tube_name();
    
    beanstalk_proxy.use_tube(&tube_name).await.unwrap();
    beanstalk_proxy.watch_tube(&tube_name).await.unwrap();
    
    tube_name
}

fn random_testing_tube_name() -> String {
    format!("test-tube.{}", fastrand::u32(..))
}

async fn run_testing_code(mut beanstalk: Beanstalk, testing_code: impl core::future::Future<Output = ()>) {
    tokio::select! {
        _ = beanstalk.run_channel() => assert!(false, "Client should not end before the testing code"),
        _ = testing_code => {},
    };
}

fn job_id_from_put_result(put_result:  &String) -> JobId {
    let extract_id_regex = Regex::new(r"INSERTED (\d+)\r\n").unwrap();
    let caps = extract_id_regex.captures(put_result).unwrap();
    caps.get(1).unwrap().as_str().parse::<JobId>().unwrap()
}