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
use log::{debug, error, info, warn};
use pulsar::{
    Authentication, Error, ProducerOptions, Pulsar, TokioExecutor,
    error::{ConnectionError, ProducerError},
    producer::SendFuture,
};

type SharedPercentiles = Arc<Mutex<Percentiles<f64>>>;

pub fn produce(
    topic: String,
    service_url: String,
    token: Option<String>,
    spawn_latency: bool,
    rate: u32,
    num_messages: u32,
) {
    let perc: SharedPercentiles = Arc::new(Mutex::new(Percentiles::new()));
    let perc_clone = perc.clone();
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let messages_count = Arc::new(AtomicI64::new(0));
    let messages_count_clone = messages_count.clone();
    let spawn_perc: Option<SharedPercentiles> = if spawn_latency {
        Some(Arc::new(Mutex::new(Percentiles::new())))
    } else {
        None
    };
    let spawn_perc_clone = spawn_perc.clone();

    let mut threads = Vec::with_capacity(2);
    threads.push(
        std::thread::Builder::new()
            .name("producer".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let client = match token {
                        None => Pulsar::builder(service_url, TokioExecutor),
                        Some(token) => {
                            Pulsar::builder(service_url, TokioExecutor).with_auth(Authentication {
                                name: "token".into(),
                                data: token.into_bytes(),
                            })
                        }
                    }
                    .build()
                    .await
                    .unwrap();
                    send_loop(
                        client,
                        topic,
                        perc_clone,
                        spawn_perc_clone,
                        running_clone,
                        messages_count_clone,
                        rate,
                        num_messages,
                    )
                    .await
                    .unwrap();
                    Ok::<(), Error>(())
                })
                .unwrap();
            })
            .unwrap(),
    );
    threads.push(std::thread::Builder::new().name("stats".into()).spawn(move || {
        let start = Instant::now();
        while running.load(Ordering::Relaxed) {
            // TODO: make it configurable
            std::thread::sleep(Duration::from_secs(1));
            {
                let perc = perc.lock().unwrap();
                info!(
                    "Throughput: {:.3} KB/s, Latency (ms): median: {:.3} p50: {:.3}, p90: {:.3}, p99: {:.3}, p999: {:.3}, max: {:.3}",
                    messages_count.load(Ordering::Relaxed) as f64 / start.elapsed().as_millis() as f64 * 1000.0,
                    perc.median().unwrap_or(0.0),
                    perc.percentile(0.5).unwrap().unwrap_or(0.0),
                    perc.percentile(0.9).unwrap().unwrap_or(0.0),
                    perc.percentile(0.99).unwrap().unwrap_or(0.0),
                    perc.percentile(0.999).unwrap().unwrap_or(0.0),
                    perc.percentile(1.0).unwrap().unwrap_or(0.0));
            }
            let spawn_perc = spawn_perc.clone();
            if let Some(spawn_perc) = spawn_perc {
                let spawn_perc = spawn_perc.lock().unwrap();
                info!("Spawn latency (ms): p50: {:.3}, p90: {:.3}, p99: {:.3}",
                    spawn_perc.percentile(0.5).unwrap().unwrap_or(0.0),
                    spawn_perc.percentile(0.9).unwrap().unwrap_or(0.0),
                    spawn_perc.percentile(0.99).unwrap().unwrap_or(0.0));
            }
        }
    }).unwrap());
    for t in threads {
        t.join().unwrap();
    }
}

async fn send_loop(
    client: Pulsar<TokioExecutor>,
    topic: String,
    perc: SharedPercentiles,
    spawn_perc: Option<SharedPercentiles>,
    running: Arc<AtomicBool>,
    messages_count: Arc<AtomicI64>,
    rate: u32,
    num_messages: u32,
) -> Result<(), Error> {
    let mut producer = client
        .producer()
        .with_topic(topic)
        .with_options(ProducerOptions {
            block_queue_if_full: true,
            ..Default::default()
        })
        .build()
        .await?;

    let rl = RateLimiter::new(rate as usize);
    for i in 0..num_messages {
        // TODO: make the payload configurable or read from a file
        let payload = vec!['a' as u8; 1024]; // 1KB payload

        // We have to await first, otherwise the producer cannot be borrowed in the next send
        let index = i;
        let perc = perc.clone();
        rl.acquire().await;
        let start = Instant::now();
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
                let spawn_perc = spawn_perc.clone();
                tokio::spawn(async move {
                    if let Some(spawn_perc) = spawn_perc {
                        let mut spawn_perc = spawn_perc.lock().unwrap();
                        let elapsed_ms = start.elapsed().as_micros() as f64 / 1000.0;
                        spawn_perc.add(elapsed_ms);
                    }
                    handle_send(send_future, start, index.into(), perc).await;
                });
            }
        }
        messages_count.fetch_add(1, Ordering::Relaxed);
    }

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
