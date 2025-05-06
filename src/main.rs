mod command;
mod stats;
mod utils;
mod tcp_worker;
mod websocket_worker;

use crate::command::{new_command, parse_config};
use crate::stats::Stats;
use crate::tcp_worker::tcp_worker;
use crate::websocket_worker::websocket_worker;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let matches = new_command();
    let workers = *matches.get_one::<usize>("workers").unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;

    rt.block_on(async_main(matches))
}

async fn async_main(matches: clap::ArgMatches) -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = parse_config(&matches);
    let stats = Arc::new(Stats::new());
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

    // 定时输出实时统计数据
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

    // 准备任务
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
        println!(
            "Warming up for {} seconds...",
            config.warmup_duration.as_secs()
        );
    }
    // 等待热身完成
    time::sleep(config.warmup_duration).await;
    // 重置统计数据
    stats.end_warmup();
    // 输出日志
    if !config.quiet {
        println!(
            "Warmup completed. Starting benchmark for {} seconds...",
            config.duration.as_secs()
        );
    }

    // 等待压力测试完成
    time::sleep(config.duration).await;
    // 标记压测结束
    stats.set_shutting_down();
    // 通知压测结束
    let _ = shutdown_tx.send(());
    // 等待所有任务完成
    while let Some(result) = tasks.next().await {
        if let Err(e) = result? {
            if !config.quiet {
                eprintln!("Task error: {}", e);
            }
        }
    }

    // 输出统计结果
    print_final_stats(&stats, config.duration, !config.quiet);
    Ok(())
}

fn print_final_stats(stats: &Stats, duration: Duration, show_output: bool) {
    if !show_output {
        return;
    }

    let hist = stats.latency_histogram.lock();
    let total_bytes = stats.total_bytes_sent.load(Ordering::Relaxed)
        + stats.total_bytes_received.load(Ordering::Relaxed);
    let total_requests = stats.total_requests.load(Ordering::Relaxed);
    let qps = total_requests as f64 / duration.as_secs_f64();
    let error_rate = stats.connection_errors.load(Ordering::Relaxed) as f64
        / (stats.total_requests.load(Ordering::Relaxed) as f64 * 100.0);

    println!("\n=== Final Results ===");
    println!("Duration:          {:.2}s", duration.as_secs_f64());
    println!(
        "Total Connections: {}",
        stats.total_connections.load(Ordering::Relaxed)
    );
    println!(
        "Success Rate:      {:.1}%",
        stats.success_connections.load(Ordering::Relaxed) as f64
            / stats.total_connections.load(Ordering::Relaxed) as f64
            * 100.0
    );
    println!("Total Requests:    {}", total_requests);
    println!("Error Rate:        {:.2}%", error_rate);
    println!("Requests Rate:     {:.2} req/s", qps);
    println!(
        "Throughput:        {:.2} MB",
        total_bytes as f64 / 1_000_000.0
    );
    println!(
        "Bandwidth:         {:.2} MB/s",
        total_bytes as f64 / duration.as_secs_f64() / 1_000_000.0
    );
    println!("Latency Distribution (us):");
    println!("  Avg: {:8.1}  Min: {:8}", hist.mean(), hist.min());
    println!(
        "  P50: {:8}  P90: {:8}",
        hist.value_at_percentile(50.0),
        hist.value_at_percentile(90.0)
    );
    println!(
        "  P95: {:8}  P99: {:8}",
        hist.value_at_percentile(95.0),
        hist.value_at_percentile(99.0)
    );
    println!("  Max: {:8}", hist.max());
}
