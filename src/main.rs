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

use std::time::{Duration, Instant};

use log::{Record, error, info, warn};
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

#[tokio::main]
async fn main() -> Result<(), Error> {
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

    for i in 0..100 {
        // TODO: make the payload configurable or read from a file
        let payload = vec![i; 1024]; // 1KB payload

        // We have to await first, otherwise the producer cannot be borrowed in the next send
        let start = Instant::now();
        let index = i;
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
                tokio::spawn(handle_send(send_future, start, index.into()));
            }
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    info!("Finished sleeping");

    Ok(())
}

async fn handle_send(future: SendFuture, start: Instant, index: usize) {
    match future.await {
        Err(e) => {
            error!("Failed to send {}: {:?}", index, e);
        }
        Ok(receipt) => {
            let elapsed = start.elapsed().as_millis();
            info!(
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
                elapsed
            );
        }
    }
}
