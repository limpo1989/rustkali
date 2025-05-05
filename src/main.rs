use clap::{Arg, ArgAction, Command, value_parser};
use futures::stream::FuturesUnordered;
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use rand::{Rng};
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::distr::Alphanumeric;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Clone)]
struct BenchmarkConfig {
    duration: Duration,
    warmup_duration: Duration,
    message_size: usize,
    quiet: bool,
    connections: u64,
    connect_rate: u64,
    connect_timeout: Duration,
    channel_lifetime: Option<Duration>,
    channel_bandwidth: Option<u64>,
    first_message: Option<Vec<u8>>,
    message: Option<Vec<u8>>,
    message_rate: Option<u64>,
    latency_marker: Option<String>,
    use_websocket: bool,
}

struct Stats {
    total_connections: AtomicU64,
    success_connections: AtomicU64,
    total_requests: AtomicU64,
    total_bytes_sent: AtomicU64,
    total_bytes_received: AtomicU64,
    latency_histogram: parking_lot::Mutex<Histogram<u64>>,
    is_warmup: parking_lot::Mutex<bool>,
    is_shutting_down: AtomicBool,
    last_print_time: parking_lot::Mutex<Instant>,
    last_print_count: parking_lot::Mutex<u64>,
    connection_errors: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        let hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
            .expect("Failed to create histogram");

        Self {
            total_connections: AtomicU64::new(0),
            success_connections: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            total_bytes_sent: AtomicU64::new(0),
            total_bytes_received: AtomicU64::new(0),
            latency_histogram: parking_lot::Mutex::new(hist),
            is_warmup: parking_lot::Mutex::new(true),
            is_shutting_down: AtomicBool::new(false),
            last_print_time: parking_lot::Mutex::new(Instant::now()),
            last_print_count: parking_lot::Mutex::new(0),
            connection_errors: AtomicU64::new(0),
        }
    }

    fn record_latency(&self, latency_us: u64) {
        if !*self.is_warmup.lock() {
            let mut hist = self.latency_histogram.lock();
            hist.record(latency_us).unwrap_or_else(|e| {
                if !self.is_shutting_down() {
                    eprintln!("Failed to record latency: {}", e);
                }
            });
        }
    }

    fn record_request(&self, bytes_sent: usize, bytes_received: usize) {
        if !*self.is_warmup.lock() {
            self.total_requests.fetch_add(1, Ordering::Relaxed);
            self.total_bytes_sent.fetch_add(bytes_sent as u64, Ordering::Relaxed);
            self.total_bytes_received.fetch_add(bytes_received as u64, Ordering::Relaxed);
        }
    }

    fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn end_warmup(&self) {
        *self.is_warmup.lock() = false;
        self.total_requests.store(0, Ordering::Relaxed);
        self.total_bytes_sent.store(0, Ordering::Relaxed);
        self.total_bytes_received.store(0, Ordering::Relaxed);
        self.latency_histogram.lock().reset();
        *self.last_print_time.lock() = Instant::now();
        *self.last_print_count.lock() = 0;
    }

    fn set_shutting_down(&self) {
        self.is_shutting_down.store(true, Ordering::Relaxed);
    }

    fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Relaxed)
    }

    fn get_qps(&self) -> f64 {
        let now = Instant::now();
        let mut last_time = self.last_print_time.lock();
        let mut last_count = self.last_print_count.lock();

        let elapsed = now.duration_since(*last_time).as_secs_f64();
        let current_count = self.total_requests.load(Ordering::Relaxed);
        let qps = (current_count - *last_count) as f64 / elapsed;

        *last_time = now;
        *last_count = current_count;

        qps
    }
}

async fn read_echo_response(
    stream: &mut TcpStream,
    expected_len: usize,
) -> Result<Option<()>, Box<dyn Error + Send + Sync>> {
    let mut buffer = vec![0u8; expected_len];

    match stream.read_exact(&mut buffer).await {
        Ok(_) => Ok(Some(())),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    if s.len() < 2 {
        return Err("Duration string too short".to_string());
    }

    if s.ends_with("ms") {
        let num: u64 = s[..s.len()-2].parse().map_err(|_| "Invalid duration number".to_string())?;
        return Ok(Duration::from_millis(num));
    }

    let (num, unit) = s.split_at(s.len() - 1);
    let num: u64 = num.parse().map_err(|_| "Invalid duration number".to_string())?;

    match unit {
        "s" => Ok(Duration::from_secs(num)),
        "m" => Ok(Duration::from_secs(num * 60)),
        "h" => Ok(Duration::from_secs(num * 3600)),
        "d" => Ok(Duration::from_secs(num * 86400)),
        _ => Err("Invalid duration unit".to_string()),
    }
}

fn parse_rate(s: &str) -> Result<u64, String> {
    if s.is_empty() {
        return Err("Empty rate string".to_string());
    }

    if s.ends_with('k') {
        let num: u64 = s[..s.len() - 1].parse().map_err(|_| "Invalid rate number".to_string())?;
        Ok(num * 1000)
    } else {
        s.parse().map_err(|_| "Invalid rate number".to_string())
    }
}

fn parse_bandwidth(s: &str) -> Result<u64, String> {
    if s.is_empty() {
        return Err("Empty bandwidth string".to_string());
    }

    let s = s.to_lowercase();
    if s.ends_with("kbps") {
        let num: u64 = s[..s.len() - 4].parse().map_err(|_| "Invalid bandwidth number".to_string())?;
        Ok(num * 1000)
    } else if s.ends_with("mbps") {
        let num: u64 = s[..s.len() - 4].parse().map_err(|_| "Invalid bandwidth number".to_string())?;
        Ok(num * 1_000_000)
    } else if s.ends_with("kbps") {
        let num: u64 = s[..s.len() - 3].parse().map_err(|_| "Invalid bandwidth number".to_string())?;
        Ok(num * 1000 * 8)
    } else if s.ends_with("mbps") {
        let num: u64 = s[..s.len() - 3].parse().map_err(|_| "Invalid bandwidth number".to_string())?;
        Ok(num * 1_000_000 * 8)
    } else {
        s.parse().map_err(|_| "Invalid bandwidth number".to_string())
    }
}

fn unescape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('0') => result.push('\0'),
                Some('x') => {
                    // Handle hex escape sequences like \x41
                    let hex_digits: String = chars.by_ref().take(2).collect();
                    if hex_digits.len() == 2 {
                        if let Ok(byte) = u8::from_str_radix(&hex_digits, 16) {
                            result.push(byte as char);
                        } else {
                            result.push_str(&format!("\\x{}", hex_digits));
                        }
                    } else {
                        result.push_str("\\x");
                        result.push_str(&hex_digits);
                    }
                }
                Some(c) => {
                    // Unknown escape sequence, keep both characters
                    result.push('\\');
                    result.push(c);
                }
                None => result.push('\\'), // Backslash at end of string
            }
        } else {
            result.push(c);
        }
    }

    result
}

fn get_message_arg(matches: &clap::ArgMatches, arg_name: &str, unescape: bool) -> Option<Vec<u8>> {
    matches.get_one::<String>(arg_name).map(|s| {
        if unescape {
            unescape_string(s).into_bytes()
        } else {
            s.as_bytes().to_vec()
        }
    })
}

fn get_file_arg(matches: &clap::ArgMatches, arg_name: &str, unescape: bool) -> Option<Vec<u8>> {
    matches.get_one::<String>(arg_name).and_then(|filename| {
        match std::fs::read_to_string(filename) {
            Ok(content) => {
                if unescape {
                    Some(unescape_string(&content).into_bytes())
                } else {
                    Some(content.into_bytes())
                }
            }
            Err(e) => {
                eprintln!("Failed to read file {}: {}", filename, e);
                None
            }
        }
    })
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    
    let matches = Command::new("rustkali")
        .version("0.1.0")
        .about("A load testing tool for WebSocket and TCP servers")
        .arg(Arg::new("host:port")
            .required(true)
            .num_args(1..)
            .help("Target server(s) in host:port format"))
        .arg(Arg::new("websocket")
            .long("websocket")
            .alias("ws")
            .action(ArgAction::SetTrue)
            .help("Use RFC6455 WebSocket transport"))
        .arg(Arg::new("connections")
            .short('c')
            .long("connections")
            .value_name("N")
            .default_value("1")
            .value_parser(value_parser!(u64))
            .help("Connections to keep open to the destinations"))
        .arg(Arg::new("connect-rate")
            .long("connect-rate")
            .value_name("R")
            .default_value("100")
            .value_parser(parse_rate)
            .help("Limit number of new connections per second"))
        .arg(Arg::new("connect-timeout")
            .long("connect-timeout")
            .value_name("T")
            .default_value("1s")
            .value_parser(parse_duration)
            .help("Limit time spent in a connection attempt"))
        .arg(Arg::new("channel-lifetime")
            .long("channel-lifetime")
            .value_name("T")
            .value_parser(parse_duration)
            .help("Shut down each connection after T seconds"))
        .arg(Arg::new("channel-bandwidth")
            .long("channel-bandwidth")
            .value_name("Bw")
            .value_parser(parse_bandwidth)
            .help("Limit single connection bandwidth"))
        .arg(Arg::new("workers")
            .short('w')
            .long("workers")
            .value_name("N")
            .default_value("24")
            .value_parser(value_parser!(usize))
            .help("Number of Tokio worker threads to use"))
        .arg(Arg::new("duration")
            .short('T')
            .long("duration")
            .value_name("T")
            .default_value("10s")
            .value_parser(parse_duration)
            .help("Load test for the specified amount of time"))
        .arg(Arg::new("unescape-message-args")
            .short('e')
            .long("unescape-message-args")
            .action(ArgAction::SetTrue)
            .help("Unescape the following {-m|-f|--first-*} arguments"))
        .arg(Arg::new("first-message")
            .long("first-message")
            .value_name("string")
            .help("Send this message first, once"))
        .arg(Arg::new("first-message-file")
            .long("first-message-file")
            .value_name("name")
            .help("Read the first message from a file"))
        .arg(Arg::new("message")
            .short('m')
            .long("message")
            .value_name("string")
            .help("Message to repeatedly send to the remote"))
        .arg(Arg::new("message-size")
            .short('s')
            .long("message-size")
            .default_value("128")
            .value_parser(value_parser!(usize))
            .help("Random message to repeatedly send to the remote"))
        .arg(Arg::new("message-file")
            .short('f')
            .long("message-file")
            .value_name("name")
            .help("Read message to send from a file"))
        .arg(Arg::new("message-rate")
            .short('r')
            .long("message-rate")
            .value_name("R")
            .value_parser(parse_rate)
            .help("Messages per second to send in a connection"))
        .arg(Arg::new("latency-marker")
            .long("latency-marker")
            .value_name("string")
            .help("Measure latency using a per-message marker"))
        .arg(Arg::new("quiet")
            .short('q')
            .action(ArgAction::SetTrue)
            .help("Suppress real-time output"))
        .get_matches();

    let workers = *matches.get_one::<usize>("workers").unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;

    rt.block_on(async_main(matches))
}

async fn async_main(matches: clap::ArgMatches) -> Result<(), Box<dyn Error + Send + Sync>> {
    let unescape = matches.get_flag("unescape-message-args");
    
    let config = BenchmarkConfig {
        duration: *matches.get_one::<Duration>("duration").unwrap(),
        warmup_duration: Duration::from_secs(3),
        quiet: matches.get_flag("quiet"),
        connections: *matches.get_one::<u64>("connections").unwrap(),
        connect_rate: *matches.get_one::<u64>("connect-rate").unwrap(),
        connect_timeout: *matches.get_one::<Duration>("connect-timeout").unwrap(),
        channel_lifetime: matches.get_one::<Duration>("channel-lifetime").cloned(),
        channel_bandwidth: matches.get_one::<u64>("channel-bandwidth").cloned(),
        first_message: get_message_arg(&matches, "first-message", unescape)
            .or_else(|| get_file_arg(&matches, "first-message-file", unescape)),
        message: get_message_arg(&matches, "message", unescape)
            .or_else(|| get_file_arg(&matches, "message-file", unescape)),
        message_size: *matches.get_one::<usize>("message-size").unwrap(),
        message_rate: matches.get_one::<u64>("message-rate").cloned(),
        latency_marker: matches.get_one::<String>("latency-marker").cloned(),
        use_websocket: matches.get_flag("websocket"),
    };

    let stats = Arc::new(Stats::new());
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

    if !config.quiet {
        let stats_clone = stats.clone();
        let config_clone = config.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if !*stats_clone.is_warmup.lock() && !config_clone.quiet {
                            let qps = stats_clone.get_qps();
                            let current_count = stats_clone.total_requests.load(Ordering::Relaxed);

                            let hist = stats_clone.latency_histogram.lock();
                            println!(
                                "[Live] QPS: {:.0} | Req: {} | Latency(us): P50={} P95={} P99={}",
                                qps,
                                current_count,
                                hist.value_at_percentile(50.0),
                                hist.value_at_percentile(95.0),
                                hist.value_at_percentile(99.0)
                            );
                        }
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        });
    }

    let mut tasks = FuturesUnordered::new();

    for target in matches.get_many::<String>("host:port").unwrap() {
        for _ in 0..config.connections {
            let task_config = config.clone();
            let task_stats = stats.clone();
            let target = target.clone();
            let shutdown = shutdown_tx.subscribe();

            tasks.push(tokio::spawn(async move {
                if task_config.connect_rate > 0 {
                    let delay = Duration::from_secs(1) / task_config.connect_rate as u32;
                    time::sleep(delay).await;
                }

                if task_config.use_websocket {
                    websocket_worker(&target, task_config, task_stats, shutdown).await
                } else {
                    tcp_worker(&target, task_config, task_stats, shutdown).await
                }
            }));
        }
    }

    if !config.quiet {
        println!("Warming up for {} seconds...", config.warmup_duration.as_secs());
    }
    time::sleep(config.warmup_duration).await;
    stats.end_warmup();
    if !config.quiet {
        println!("Warmup completed. Starting benchmark for {} seconds...", config.duration.as_secs());
    }

    time::sleep(config.duration).await;
    stats.set_shutting_down();
    let _ = shutdown_tx.send(());

    while let Some(result) = tasks.next().await {
        if let Err(e) = result? {
            if !config.quiet {
                eprintln!("Task error: {}", e);
            }
        }
    }

    print_final_stats(&stats, config.duration, !config.quiet);
    Ok(())
}

async fn tcp_worker(
    target: &str,
    config: BenchmarkConfig,
    stats: Arc<Stats>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    stats.total_connections.fetch_add(1, Ordering::Relaxed);

    let mut stream = match time::timeout(config.connect_timeout, TcpStream::connect(target)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            if !stats.is_shutting_down() && !config.quiet {
                eprintln!("Failed to connect to {}: {}", target, e);
            }
            stats.record_connection_error();
            return Ok(());
        }
        Err(_) => {
            if !stats.is_shutting_down() && !config.quiet {
                eprintln!("Connection timeout to {}", target);
            }
            stats.record_connection_error();
            return Ok(());
        }
    };

    if let Some(first_msg) = &config.first_message {
        if let Err(e) = stream.write_all(first_msg).await {
            if !stats.is_shutting_down() && !config.quiet {
                eprintln!("Failed to send first message: {}", e);
            }
            stats.record_connection_error();
            return Ok(());
        }
    }

    let mut payload = if let Some(msg) = &config.message {
        msg.clone()
    } else {
        generate_payload(config.message_size)
    };
    let mut counter = 0;

    let start_time = Instant::now();
    loop {
        if let Some(lifetime) = config.channel_lifetime {
            if start_time.elapsed() >= lifetime {
                break;
            }
        }

        tokio::select! {
            _ = async {
                if stats.is_shutting_down() {
                    return;
                }

                let message_size = if let Some(bw) = config.channel_bandwidth {
                    std::cmp::min(payload.len(), bw as usize / 8)
                } else {
                    payload.len()
                };

                let message = if let Some(latency_marker) = &config.latency_marker {
                    latency_marker.as_bytes().to_vec()
                } else {
                    payload[..message_size].to_vec()
                };
                
                let message_len = message.len();

                if let Err(e) = stream.write_all(&message).await {
                    if !stats.is_shutting_down() && !config.quiet {
                        eprintln!("Write error: {}", e);
                    }
                    stats.record_connection_error();
                    return;
                }

                let start = Instant::now();

                match read_echo_response(&mut stream, message_len).await {
                    Ok(Some(_)) => {
                        let latency = start.elapsed().as_micros() as u64;
                        stats.record_latency(latency);
                        stats.record_request(message_len, message_len);
                    }
                    Ok(None) => return,
                    Err(e) => {
                        if !stats.is_shutting_down() && !config.quiet {
                            eprintln!("Read error: {}", e);
                        }
                        stats.record_connection_error();
                        return;
                    }
                }

                counter += 1;
                if counter % 1000 == 0 {
                    tokio::task::yield_now().await;
                    if config.message.is_none() {
                        payload = generate_payload(config.message_size);
                    }
                }

                if let Some(rate) = config.message_rate {
                    let target_duration = Duration::from_secs_f64(1.0 / rate as f64);
                    let elapsed = start.elapsed();
                    if elapsed < target_duration {
                        time::sleep(target_duration - elapsed).await;
                    }
                }
            } => {},
            _ = shutdown.recv() => break,
        }
    }

    stats.success_connections.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

async fn websocket_worker(
    target: &str,
    config: BenchmarkConfig,
    stats: Arc<Stats>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    stats.total_connections.fetch_add(1, Ordering::Relaxed);

    let (ws_stream, _) = match time::timeout(config.connect_timeout, connect_async(target)).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            if !stats.is_shutting_down() && !config.quiet {
                eprintln!("Failed to connect to WebSocket {}: {}", target, e);
            }
            stats.record_connection_error();
            return Ok(());
        }
        Err(_) => {
            if !stats.is_shutting_down() && !config.quiet {
                eprintln!("WebSocket connection timeout to {}", target);
            }
            stats.record_connection_error();
            return Ok(());
        }
    };
    let (mut write, mut read) = ws_stream.split();
    let mut counter = 0;

    if let Some(first_msg) = &config.first_message {
        if let Err(e) = write.send(Message::Binary(first_msg.clone().into())).await {
            if !stats.is_shutting_down() && !config.quiet {
                eprintln!("Failed to send first WebSocket message: {}", e);
            }
            stats.record_connection_error();
            return Ok(());
        }
    }

    let start_time = Instant::now();
    loop {
        if let Some(lifetime) = config.channel_lifetime {
            if start_time.elapsed() >= lifetime {
                break;
            }
        }

        tokio::select! {
            _ = async {
                if stats.is_shutting_down() {
                    return;
                }

                let message = if let Some(msg) = &config.message {
                    msg.clone()
                } else if let Some(latency_marker) = &config.latency_marker {
                    latency_marker.as_bytes().to_vec()
                } else {
                    generate_payload(config.message_size)
                };

                if let Err(e) = write.send(Message::Binary(message.clone().into())).await {
                    if !stats.is_shutting_down() && !config.quiet {
                        eprintln!("WebSocket send error: {}", e);
                    }
                    stats.record_connection_error();
                    return;
                }

                let start = Instant::now();
                match read.next().await {
                    Some(Ok(Message::Binary(data))) => {
                        let latency = start.elapsed().as_micros() as u64;
                        stats.record_latency(latency);
                        stats.record_request(message.len(), data.len());
                    }
                    Some(Err(e)) => {
                        if !stats.is_shutting_down() && !config.quiet {
                            eprintln!("WebSocket receive error: {}", e);
                        }
                        stats.record_connection_error();
                        return;
                    }
                    None => return,
                    _ => {}
                }

                counter += 1;
                if counter % 1000 == 0 {
                    tokio::task::yield_now().await;
                }

                if let Some(rate) = config.message_rate {
                    let target_duration = Duration::from_secs_f64(1.0 / rate as f64);
                    let elapsed = start.elapsed();
                    if elapsed < target_duration {
                        time::sleep(target_duration - elapsed).await;
                    }
                }
            } => {},
            _ = shutdown.recv() => break,
        }
    }

    stats.success_connections.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn generate_payload(size: usize) -> Vec<u8> {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect()
}

fn print_final_stats(stats: &Stats, duration: Duration, show_output: bool) {
    if !show_output {
        return;
    }

    let hist = stats.latency_histogram.lock();
    let total_bytes = stats.total_bytes_sent.load(Ordering::Relaxed) + stats.total_bytes_received.load(Ordering::Relaxed);
    let total_requests = stats.total_requests.load(Ordering::Relaxed);
    let qps = total_requests as f64 / duration.as_secs_f64();
    let error_rate = stats.connection_errors.load(Ordering::Relaxed) as f64 / (stats.total_requests.load(Ordering::Relaxed) as f64 * 100.0);

    println!("\n=== Final Results ===");
    println!("Duration:          {:.2}s", duration.as_secs_f64());
    println!("Total Connections: {}", stats.total_connections.load(Ordering::Relaxed));
    println!("Success Rate:      {:.1}%", stats.success_connections.load(Ordering::Relaxed) as f64 / stats.total_connections.load(Ordering::Relaxed) as f64 * 100.0);
    println!("Total Requests:    {}", total_requests);
    println!("Error Rate:        {:.2}%", error_rate);
    println!("Requests Rate:     {:.2} req/s", qps);
    println!("Throughput:        {:.2} MB", total_bytes as f64 / 1_000_000.0);
    println!("Bandwidth:         {:.2} MB/s", total_bytes as f64 / duration.as_secs_f64() / 1_000_000.0);
    println!("Latency Distribution (us):");
    println!("  Avg: {:8.1}  Min: {:8}", hist.mean(), hist.min());
    println!("  P50: {:8}  P90: {:8}", hist.value_at_percentile(50.0), hist.value_at_percentile(90.0));
    println!("  P95: {:8}  P99: {:8}", hist.value_at_percentile(95.0), hist.value_at_percentile(99.0));
    println!("  Max: {:8}", hist.max());
}