use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use serde_yaml;
use log;

/// Queue size limit for messages to a Beanstalk channel
const BEANSTALK_MESSAGE_QUEUE_SIZE: usize = 128;
pub const DEFAULT_PRIORITY: u32 = 0;
pub const PUT_DEFAULT_DELAY: u32 = 0;
pub const DEFAULT_TIME_TO_RUN: u32 = 60;
pub const RESERVE_DEFAULT_TIMEOUT: u32 = 60;

/// A beanstalkd handle
pub struct Beanstalk {
    /// TCP stream to beanstalkd
    stream: TcpStream,
    /// The receiving end for messages, used by run_channel
    rx: mpsc::Receiver<ClientMessage>,
    /// The transmitting end, use by whatever to interact with beanstalkd
    tx: mpsc::Sender<ClientMessage>,
}

/// A beanstalkd error
#[derive(Error, Debug)]
pub enum BeanstalkError {
    #[error("the internal queue to Beanstalk is not available: {0}")]
    QueueUnvailable(String),
    #[error("a return channel has failed to receive a Beanstalk response: {0}")]
    ReturnChannelFailure(String),
    #[error("unexpected response from Beanstalk for command {0}: {1}")]
    UnexpectedResponse(String, String),
    #[error("beanstalk communication error: {0}")]
    CommunicationError(String),
    #[error("job reservation timeout")]
    ReservationTimeout,
}

/// Convenience struct: copy of the channel's transmissiting end, with some methods
#[derive(Clone)]
pub struct BeanstalkProxy {
    request_tx: mpsc::Sender<ClientMessage>
}

/// Convenience type: Beanstalk operation result
pub type BeanstalkResult = Result<String, BeanstalkError>;

/// A command which can be sent over the Beanstalk channel
struct ClientMessage {
    /// A oneshot for the reply
    return_tx: oneshot::Sender<BeanstalkResult>,
    /// The actual command body
    body: ClientMessageBody,
}

struct ClientMessageBody {
    command: String,
    more_condition: Option<String>,
}

impl Beanstalk {
    /// Connects to beanstalkd
    pub async fn connect(addr: &String) -> std::io::Result<Self> {
        log::debug!("connecting to beanstalkd at {}", addr);
        TcpStream::connect(addr).await.map(|stream| {
            let (tx, rx) = mpsc::channel::<ClientMessage>(BEANSTALK_MESSAGE_QUEUE_SIZE);
            log::debug!("connected to beanstalkd at {}", addr);
            Self { stream, rx, tx }
        })
    }

    /// Provides a clone of the channel's tx, for use by any task
    pub fn proxy(&self) -> BeanstalkProxy {
        BeanstalkProxy { request_tx: self.tx.clone() }
    }

    /// The channel which owns the actual connection and processes messages
    /// Note the &mut: by taking a mut reference, this function
    /// prevents anything else from altering the Beanstalk struct
    pub async fn run_channel(&mut self) {
        log::debug!("running beanstalkd channel");

        let (read, mut write) = self.stream.split();
        let mut bufreader = BufReader::new(read);

        while let Some(message) = self.rx.recv().await {
            /* Send the command to beanstalk and get the first response line */
            let mut response = String::new();
            let response_status = write.write_all(message.body.command.as_bytes()).await
                .and(bufreader.read_line(&mut response).await);

            /* Make sure we actually got a response, otherwise tell the other task it failed */
            if let Err(e) = response_status {
                message.return_tx.send(Err(BeanstalkError::CommunicationError(e.to_string()))).ok();
                continue;
            }

            /* Figure out if we need to read more: the task is expecting a prefix AND that's what we get */
            let mut response_parts = response.trim().split(" ");
            let expect_more_content = message.body.more_condition
                .map(|expected_prefix| response_parts.next().map(|prefix_received| expected_prefix == prefix_received))
                .flatten().unwrap_or(false); /* default to false on Nones: no prefix or no first "part" in the response */

            /* No more content, reply with the first line alone and move on */
            if !expect_more_content {
                message.return_tx.send(Ok(response)).ok();
                continue;
            }

            /* Alright, more content: try to figure out how many bytes we need to read
             * That's the last item in the first response line above */
            let extra_payload_length = response_parts.last()
                .map(|bytes_str| bytes_str.parse::<usize>().ok())
                .flatten();
            let extra_payload_length = match extra_payload_length {
                Some(length) => length,
                None => {
                    /* Either there was no "last" item or it wasn't an int */
                    message.return_tx.send(Err(BeanstalkError::UnexpectedResponse("reserve".to_string(), response.clone()))).ok();
                    continue;
                }
            };

            /* Let's get that extra payload now and reply */
            let mut extra_payload_buffer = vec![0 as u8; extra_payload_length + 2];
            let extra_read_status = bufreader.read_exact(&mut extra_payload_buffer).await;
            message.return_tx.send(
                extra_read_status
                    /* we got something back: append it to the first line we already have and send the lot */
                    .map(|_| format!("{}{}", response, String::from_utf8_lossy(&extra_payload_buffer).trim().to_string()))
                    /* we couldn't get the extra payload: reply with an error */
                    .map_err(|e| BeanstalkError::CommunicationError(e.to_string()))
            ).ok();
        }
    }
}

/// A beanstalk job
pub struct Job {
    pub id: u64,
    pub payload: String
}

pub struct PutCommandConfig {
    pub priority: u32,
    pub delay: u32,
    pub time_to_run: u32
}

impl PutCommandConfig {
    pub fn new(priority: Option<u32>, delay: Option<u32>, time_to_run: Option<u32>) -> Self {
        Self {
            priority: priority.unwrap_or(DEFAULT_PRIORITY),
            delay: delay.unwrap_or(PUT_DEFAULT_DELAY),
            time_to_run: time_to_run.unwrap_or(DEFAULT_TIME_TO_RUN)
        }
    }
}

pub struct ReleaseCommandConfig {
    pub priority: u32,
    pub delay: u32 
}

impl ReleaseCommandConfig {
    pub fn new(priority: Option<u32>, delay: Option<u32>) -> Self {
        Self {
            priority: priority.unwrap_or(DEFAULT_PRIORITY),
            delay: delay.unwrap_or(PUT_DEFAULT_DELAY)
        }
    }
}

/// beanstalkd statistics
#[derive(Serialize, Deserialize)]
pub struct Statistics {
    #[serde(rename = "current-jobs-ready")]
    pub jobs_ready: usize,
    #[serde(rename = "current-jobs-reserved")]
    pub jobs_reserved: usize,
    #[serde(rename = "current-jobs-delayed")]
    pub jobs_delayed: usize,
    #[serde(rename = "total-jobs")]
    pub total_jobs: usize,
    #[serde(rename = "current-connections")]
    pub current_connections: usize,
    pub uptime: u64,
}

/// job statistics
#[derive(Serialize, Deserialize)]
pub struct JobStatistics {
    pub id: u64,
    pub tube: String,
    pub state: String, // "ready" or "delayed" or "reserved" or "buried"
    #[serde(rename = "pri")]
    pub priority: u32,
    pub age: u32,
    pub delay: u32,
    #[serde(rename = "ttr")]
    pub time_to_run: u32,
    #[serde(rename = "time-left")]
    pub time_left: u32,
    pub file: u32,
    pub reserves: u32,
    pub timeouts: u32,
    pub releases: u32,
    pub buries: u32,
    pub kicks: u32
}

/// Tube statistics
#[derive(Serialize, Deserialize)]
pub struct TubeStatistics {
    pub name: String,
    #[serde(rename = "current-jobs-urgent")]
    pub jobs_urgent: u64,
    #[serde(rename = "current-jobs-ready")]
    pub jobs_ready: u64,
    #[serde(rename = "current-jobs-reserved")]
    pub jobs_reserved: u64,
    #[serde(rename = "current-jobs-delayed")]
    pub jobs_delayed: u64,
    #[serde(rename = "current-jobs-buried")]
    pub jobs_buried: u64,
    #[serde(rename = "total-jobs")]
    pub total_jobs: u64,
    #[serde(rename = "current-using")]
    pub using: usize,
    #[serde(rename = "current-waiting")]
    pub waiting: usize,
    #[serde(rename = "current-watching")]
    pub watching: usize,
    pub pause: i64,
    #[serde(rename = "cmd-delete")]
    pub cmd_delete: u64,
    #[serde(rename = "cmd-pause-tube")]
    pub cmd_pause_tube: u64,
    #[serde(rename = "pause-time-left")]
    pub pause_time_left: u32
}

impl BeanstalkProxy {
    /// Low level channel exchange: send a message body over the channel and wait for a reply
    async fn exchange(&self, body: ClientMessageBody) -> BeanstalkResult {
        let (tx, rx) = oneshot::channel::<BeanstalkResult>();
        self.request_tx.send(ClientMessage { return_tx: tx, body }).await
            .map_err(|e| BeanstalkError::QueueUnvailable(e.to_string()))?;
        rx.await.map_err(|e| BeanstalkError::ReturnChannelFailure(e.to_string()))?
    }

    /// Ask beanstalk to USE a tube on this connection
    pub async fn use_tube(&self, tube: &str) -> BeanstalkResult {
        log::debug!("using tube {}", tube);
        let using = self.exchange(ClientMessageBody { command: format!("use {}\r\n", tube), more_condition: None }).await?;
        match using.starts_with("USING ") {
            true => Ok(using),
            false => Err(BeanstalkError::UnexpectedResponse("use".to_string(), using))
        }
    }

    /// Ask beanstalk to WATCH a tube on this connection
    pub async fn watch_tube(&self, tube: &str) -> BeanstalkResult {
        log::debug!("watching tube {}", tube);
        let watching = self.exchange(ClientMessageBody { command: format!("watch {}\r\n", tube), more_condition: None }).await?;
        match watching.starts_with("WATCHING ") {
            true => Ok(watching),
            false => Err(BeanstalkError::UnexpectedResponse("watch".to_string(), watching))
        }
    }

    pub async fn ignore_tube(&self, tube: &str) -> BeanstalkResult {
        log::debug!("ignoring tube {}", tube);
        let command = format!("ignore {}\r\n", tube);
        let ignore_result = self.exchange(ClientMessageBody { command: command, more_condition: None }).await?;
        match ignore_result.starts_with("WATCHING ") {
            true => Ok(ignore_result),
            false => Err(BeanstalkError::UnexpectedResponse("ignore".to_string(), ignore_result))
        }
    }

    /// Put a job into the queue
    pub async fn put(&self, job: String) -> BeanstalkResult {
        log::debug!("putting beanstalkd job, {} byte(s)", job.len());
        let command = format!("put {} {} {} {}\r\n{}\r\n", DEFAULT_PRIORITY, PUT_DEFAULT_DELAY, DEFAULT_TIME_TO_RUN, job.len(), job);
        let inserted = self.exchange(ClientMessageBody { command, more_condition: None }).await?;
        match inserted.starts_with("INSERTED ") {
            true => Ok(inserted),
            false => Err(BeanstalkError::UnexpectedResponse("put".to_string(), inserted))
        }
    }

    /// Put a job into the queue, with a custom configuration
    pub async fn put_with_config(&self, job: String, config: PutCommandConfig) -> BeanstalkResult {
        log::debug!("putting beanstalkd job, {} byte(s)", job.len());
        let command = format!("put {} {} {} {}\r\n{}\r\n", config.priority, config.delay, config.time_to_run, job.len(), job);
        let inserted = self.exchange(ClientMessageBody { command, more_condition: None }).await?;
        match inserted.starts_with("INSERTED ") {
            true => Ok(inserted),
            false => Err(BeanstalkError::UnexpectedResponse("put".to_string(), inserted))
        }
    }

    pub async fn release_with_config(&self, job_id: u64, config: ReleaseCommandConfig) -> BeanstalkResult {
        log::debug!("releasing beanstalkd job {}", job_id);
        let command = format!("release {} {} {}\r\n", job_id, config.priority, config.delay);
        let release_response = self.exchange(ClientMessageBody { command, more_condition: None }).await?;
        match release_response.starts_with("RELEASED") {
            true => Ok(release_response),
            false => Err(BeanstalkError::UnexpectedResponse("release".to_string(), release_response))
        }
    }

    pub async fn release(&self, job_id: u64) -> BeanstalkResult {
        self.release_with_config(job_id, ReleaseCommandConfig::new(None, None)).await
    }

    /// Reserve a job from the queue. It uses the timeout `RESERVE_DEFAULT_TIMEOUT`!
    pub async fn reserve(&self) -> Result<Job, BeanstalkError> {
        self.reserve_with_timeout(RESERVE_DEFAULT_TIMEOUT).await
    }

    pub async fn reserve_with_timeout(&self, timeout: u32) -> Result<Job, BeanstalkError> {
        let command = format!("reserve-with-timeout {}\r\n", timeout);
        let command_response = self.exchange(ClientMessageBody { command, more_condition: Some("RESERVED".to_string()) }).await?;
        let mut lines = command_response.trim().split("\r\n");

        let first_line = lines.next()
            .ok_or(BeanstalkError::UnexpectedResponse("stats-tube".to_string(), "empty response".to_string()))?;
        let parts: Vec<&str> = first_line.trim().split(" ").collect();

        if parts.len() == 1 && parts[0] == "TIMED_OUT" {
            return Err(BeanstalkError::ReservationTimeout);
        }

        if parts.len() != 3 || parts[0] != "RESERVED" {
            return Err(BeanstalkError::UnexpectedResponse("reserve".to_string(), command_response));
        }

        let id = parts[1].parse::<u64>()
            .map_err(|_| BeanstalkError::UnexpectedResponse("reserve".to_string(), command_response.clone()))?;

        Ok(Job {
            id,
            payload: lines.collect::<Vec<&str>>().join("\r\n"),
        })
    }

    /// Peek a job by id
    pub async fn peek(&self, job_id: u64) -> Result<Job, BeanstalkError> {
        let command_response = self.exchange(ClientMessageBody { command: format!("peek {}\r\n", job_id), more_condition: Some("FOUND".to_string()) }).await?;
        let mut lines = command_response.trim().split("\r\n");

        let first_line = lines.next()
            .ok_or(BeanstalkError::UnexpectedResponse("peek".to_string(), "<empty response>".to_string()))?;
        let parts: Vec<&str> = first_line.trim().split(" ").collect();

        if parts.len() != 3 || parts[0] != "FOUND" {
            return Err(BeanstalkError::UnexpectedResponse("peek".to_string(), command_response));
        }

        Ok(Job {
            id: job_id,
            payload: lines.collect::<Vec<&str>>().join("\r\n"),
        })
    }

    /// Peek the next ready job, if any
    pub async fn peek_ready(&self) -> Result<Option<Job>, BeanstalkError> {
        self.peek_from_queue(String::from("ready")).await
    }

    /// Peek the delayed job with the shortest delay left
    pub async fn peek_delayed(&self) -> Result<Option<Job>, BeanstalkError> {
        self.peek_from_queue(String::from("delayed")).await
    }

    /// Peek the next buried job, if any
    pub async fn peek_buried(&self) -> Result<Option<Job>, BeanstalkError> {
        self.peek_from_queue(String::from("buried")).await
    }

    /// Delete a job from the queue
    pub async fn delete(&self, id: u64) -> BeanstalkResult {
        log::debug!("deleting job ID {}", id);
        let deleted = self.exchange(ClientMessageBody { command: format!("delete {}\r\n", id), more_condition: None }).await?;
        match deleted.starts_with("DELETED") {
            true => Ok(deleted),
            false => Err(BeanstalkError::UnexpectedResponse("delete".to_string(), deleted))
        }
    }

    /// Kick a job from the buried or delayed queue to the ready queue
    pub async fn kick_job(&self, id: u64) -> BeanstalkResult {
        log::debug!("kicking job ID {}", id);
        let kick_response = self.exchange(ClientMessageBody { command: format!("kick-job {}\r\n", id), more_condition: None }).await?;
        match kick_response.starts_with("KICKED") {
            true => Ok(kick_response),
            false => Err(BeanstalkError::UnexpectedResponse("kick-job".to_string(), kick_response))
        }
    }

    /// Kick at most `bound` jobs from the buried or delayed queue to the ready queue
    pub async fn kick(&self, bound: u64) -> BeanstalkResult {
        log::debug!("kicking {} jobs", bound);
        let kick_response = self.exchange(ClientMessageBody { command: format!("kick {}\r\n", bound), more_condition: None }).await?;
        match kick_response.starts_with("KICKED") {
            true => Ok(kick_response),
            false => Err(BeanstalkError::UnexpectedResponse("kick".to_string(), kick_response))
        }
    }

    /// Get server stats
    pub async fn stats(&self) -> Result<Statistics, BeanstalkError> {
        let command_name = String::from("stats");
        let command_response = self.exchange(ClientMessageBody { command: format!("{}\r\n", command_name), more_condition: Some("OK".to_string()) }).await?;
        
        let stats_yaml = Self::extract_stats_response(command_name, command_response)?;
        serde_yaml::from_str(&stats_yaml)
            .map_err(|e| BeanstalkError::UnexpectedResponse("stats".to_string(), e.to_string()))
    }

    /// Get the stats for a job
    pub async fn stats_job(&self, id: u64) -> Result<JobStatistics, BeanstalkError> {
        let command_name = String::from("stats-job");
        let command_response = self.exchange(ClientMessageBody { command: format!("{} {}\r\n", command_name, id), more_condition: Some("OK".to_string()) }).await?;

        let stats_yaml = Self::extract_stats_response(command_name, command_response)?;
        serde_yaml::from_str(&stats_yaml)
            .map_err(|e| BeanstalkError::UnexpectedResponse("stats-job".to_string(), e.to_string()))
    }

    /// Get the stats for a tube
    pub async fn stats_tube(&self, tube_name: &String) -> Result<TubeStatistics, BeanstalkError> {
        let command_name = String::from("stats-tube");
        let command_response = self.exchange(ClientMessageBody { command: format!("{} {}\r\n", command_name, tube_name), more_condition: Some("OK".to_string()) }).await?;

        let stats_yaml = Self::extract_stats_response(command_name, command_response)?;
        serde_yaml::from_str(&stats_yaml)
            .map_err(|e| BeanstalkError::UnexpectedResponse("stats-job".to_string(), e.to_string()))
    }

    /// Bury a job from the queue
    pub async fn bury(&self, id: u64) -> BeanstalkResult {
        log::debug!("burying job ID {}", id);
        let buried = self.exchange(ClientMessageBody { command: format!("bury {} {}\r\n", id, DEFAULT_PRIORITY), more_condition: None }).await?;
        match buried.starts_with("BURIED") {
            true => Ok(buried),
            false => Err(BeanstalkError::UnexpectedResponse("bury".to_string(), buried))
        }
    }

    /// Touch a reserved job
    pub async fn touch(&self, id: u64) -> BeanstalkResult {
        log::debug!("touching job ID {}", id);
        let touched = self.exchange(ClientMessageBody { command: format!("touch {}\r\n", id), more_condition: None }).await?;
        match touched.starts_with("TOUCHED") {
            true => Ok(touched),
            false => Err(BeanstalkError::UnexpectedResponse("touch".to_string(), touched))
        }
    }

    pub async fn list_tubes(&self) -> Result<Vec<String>, BeanstalkError> {
        log::debug!("listing tubes");
        let command = String::from("list-tubes\r\n");
        let list_result = self.exchange(ClientMessageBody { command, more_condition: Some("OK".to_string()) }).await?;
        let mut lines = list_result.trim().split("\r\n");

        let first_line = lines.next()
            .ok_or(BeanstalkError::UnexpectedResponse(String::from("list-tubes"), "<empty response>".to_string()))?;
        let parts: Vec<&str> = first_line.trim().split(" ").collect();

        if parts.len() != 2 || parts[0] != "OK" {
            return Err(BeanstalkError::UnexpectedResponse(String::from("list-tubes"), list_result));
        }
        serde_yaml::from_str(&lines.collect::<Vec<&str>>().join("\r\n"))
            .map_err(|e| BeanstalkError::UnexpectedResponse("list-tubes".to_string(), e.to_string()))
    }

    // private functions //////////////////////////////////////////////////////

    async fn peek_from_queue(&self, status: String) -> Result<Option<Job>, BeanstalkError> {
        let command_name = format!("peek-{}", status);
        let command_response = self.exchange(ClientMessageBody { command: format!("{}\r\n", command_name), more_condition: Some("FOUND".to_string()) }).await?;
        let mut lines = command_response.trim().split("\r\n");

        let first_line = lines.next()
            .ok_or(BeanstalkError::UnexpectedResponse(command_name.clone(), "<empty response>".to_string()))?;
        let parts: Vec<&str> = first_line.trim().split(" ").collect();

        match parts[0]  {
            "FOUND" => {
                if parts.len() != 3 {
                    return Err(BeanstalkError::UnexpectedResponse(command_name.clone(), command_response));
                }
        
                let id = parts[1].parse::<u64>()
                    .map_err(|_| BeanstalkError::UnexpectedResponse(command_name, command_response.clone()))?;
        
                Ok(Some(Job {
                    id: id,
                    payload: lines.collect::<Vec<&str>>().join("\r\n"),
                }))
            },
            "NOT_FOUND" => Ok(None),
            _ => Err(BeanstalkError::UnexpectedResponse("peek".to_string(), command_response))
        }
    }

    fn extract_stats_response(command_name: String, command_response: String) -> Result<String, BeanstalkError> {
        let mut lines = command_response.trim().split("\r\n");

        let first_line = lines.next()
            .ok_or(BeanstalkError::UnexpectedResponse(command_name.clone(), "empty response".to_string()))?;
        let parts: Vec<&str> = first_line.trim().split(" ").collect();

        if parts.len() != 2 || parts[0] != "OK" {
            return Err(BeanstalkError::UnexpectedResponse(command_name, command_response));
        }

        Ok(lines.collect::<Vec<&str>>().join("\r\n"))
    } 
}
