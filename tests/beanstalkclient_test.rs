use beanstalkclient::{Beanstalk, Statistics, BeanstalkProxy};
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
async fn put_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;

        let result = beanstalk_proxy.put(String::from("job-web-event")).await.unwrap();
    
        // expect that containg INSERTED followed by the id of the created job
        let regex = Regex::new(r"INSERTED \d+\r\n").unwrap();
        assert!(regex.is_match(&result));
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
async fn delete_test() {
    let (beanstalk_client, beanstalk_proxy) = setup_client().await;

    let testing_code = async {
        in_new_testing_tube(&beanstalk_proxy).await;
        
        let put_result = beanstalk_proxy.put(String::from("a-job")).await.unwrap();
        let extract_id_regex = Regex::new(r"INSERTED (\d+)\r\n").unwrap();
        let caps = extract_id_regex.captures(&put_result).unwrap();
        let job_id = caps.get(1).unwrap().as_str().parse::<u64>().unwrap();

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
        let extract_id_regex = Regex::new(r"INSERTED (\d+)\r\n").unwrap();
        let caps = extract_id_regex.captures(&put_result).unwrap();
        let job_id = caps.get(1).unwrap().as_str().parse::<u64>().unwrap();

        let job_stats = beanstalk_proxy.stats_job(job_id).await.unwrap();
        assert_eq!(job_id, job_stats.id);
        assert_eq!(tube_name, job_stats.tube);
        assert_eq!("ready", job_stats.state);
        assert_eq!(0, job_stats.pri);
        assert_eq!(0, job_stats.delay);
        assert_eq!(beanstalkclient::DEFAULT_TIME_TO_RUN, job_stats.ttr);
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