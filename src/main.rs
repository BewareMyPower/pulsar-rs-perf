// Copyright 2025 Yunze Xu
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use async_rate_limiter::RateLimiter;
use inc_stats::Percentiles;
use log::{Record, debug, error, info, warn};
use logforth::{Diagnostic, Layout, append, filter::EnvFilter};
use pulsar::{
    Error, Pulsar, TokioExecutor,
    error::{ConnectionError, ProducerError},
    producer::SendFuture,
};

#[derive(Debug)]
struct CustomLayout;

impl Layout for CustomLayout {
    fn format(&self, record: &Record, _: &[Box<dyn Diagnostic>]) -> anyhow::Result<Vec<u8>> {
        let base_file_name = record
            .file()
            .map(std::path::Path::new)
            .and_then(std::path::Path::file_name)
            .map(std::ffi::OsStr::to_string_lossy)
            .unwrap_or_default();
        Ok(format!(
            "{} [{}] {} {}:{} {}",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S.%3f"),
            std::thread::current().name().unwrap_or("unknown"),
            record.metadata().level(),
            base_file_name,
            record.line().unwrap_or(0),
            record.args()
        )
        .into_bytes())
    }
}

type SharedPercentiles = Arc<Mutex<Percentiles<f64>>>;

fn main() {
    let mut threads = Vec::new();
    let perc: SharedPercentiles = Arc::new(Mutex::new(Percentiles::new()));
    let perc_clone = perc.clone();
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let messages_count = Arc::new(AtomicI64::new(0));
    let messages_count_clone = messages_count.clone();

    threads.push(
        std::thread::Builder::new()
            .name("producer".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(send_loop(perc_clone, running_clone, messages_count_clone))
                    .unwrap();
            })
            .unwrap(),
    );
    threads.push(
        std::thread::Builder::new()
            .name("stats".into())
            .spawn(move || {
                let start = Instant::now();
                while running.load(Ordering::Relaxed) {
                    // TODO: make the interval configurable
                    std::thread::sleep(Duration::from_secs(1));
                        let perc = perc.lock().unwrap();
                        info!(
                            "Throughput: {:.3} KB/s, Latency (ms): median: {:.3} p50: {:.3}, p90: {:.3}, p99: {:.3}, p999: {:.3}, max: {:.3}",
                            messages_count.load(Ordering::Relaxed) as f64 / start.elapsed().as_millis() as f64 * 1000.0,
                            perc.median().unwrap_or(0.0),
                            perc.percentile(0.5).unwrap().unwrap_or(0.0),
                            perc.percentile(0.9).unwrap().unwrap_or(0.0),
                            perc.percentile(0.99).unwrap().unwrap_or(0.0),
                            perc.percentile(0.999).unwrap().unwrap_or(0.0),
                            perc.percentile(1.0).unwrap().unwrap_or(0.0)
                        );
                }
            })
            .unwrap(),
    );
    for t in threads {
        t.join().unwrap();
    }
}

async fn send_loop(
    perc: SharedPercentiles,
    running: Arc<AtomicBool>,
    messages_count: Arc<AtomicI64>,
) -> Result<(), Error> {
    logforth::builder()
        .dispatch(|d| {
            d.filter(EnvFilter::from_default_env())
                .append(append::Stdout::default().with_layout(CustomLayout))
        })
        .apply();

    // TODO: make the following variables configurable
    let addr = "pulsar://localhost:6650";
    let topic = "my-topic";

    let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    // TODO: make batching configurable
    let mut producer = client.producer().with_topic(topic).build().await?;

    // TODO: make the rate configurable
    let rl = RateLimiter::new(500);
    for i in 0..10000 {
        // TODO: make the payload configurable or read from a file
        let payload = vec!['a' as u8; 1024]; // 1KB payload

        // We have to await first, otherwise the producer cannot be borrowed in the next send
        let start = Instant::now();
        let index = i;
        let perc = perc.clone();
        rl.acquire().await;
        match producer.send_non_blocking(payload).await {
            Err(pulsar::Error::Producer(ProducerError::Connection(ConnectionError::SlowDown))) => {
                // TODO: support resend the message
                warn!("Failed to send {} due to pending send queue is full", index);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => {
                error!("Failed to send {}: {:?}", index, e);
            }
            Ok(send_future) => {
                // Await the send future in a separate task to avoid blocking the next send
                tokio::spawn(handle_send(send_future, start, index.into(), perc));
            }
        }
        messages_count.fetch_add(1, Ordering::Relaxed);
    }

    // TODO: use a graceful stop mechanism
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    info!("Finished sleeping");
    running.store(false, Ordering::Relaxed);

    Ok(())
}

async fn handle_send(future: SendFuture, start: Instant, index: i64, perc: SharedPercentiles) {
    match future.await {
        Err(e) => {
            error!("Failed to send {}: {:?}", index, e);
        }
        Ok(receipt) => {
            let elapsed_ms = start.elapsed().as_micros() as f64 / 1000.0;
            let mut perc = perc.lock().unwrap();
            perc.add(elapsed_ms);
            debug!(
                "Sent message {} to {} after {} ms",
                index,
                receipt
                    .message_id
                    .map(|msg_id| format!(
                        "{}:{}:{}:{}",
                        msg_id.ledger_id,
                        msg_id.entry_id,
                        msg_id.partition.unwrap_or(-1),
                        msg_id.batch_index.unwrap_or(-1)
                    ))
                    .unwrap_or(String::from("unknown")),
                elapsed_ms
            );
        }
    }
}
