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

use clap::{Parser, Subcommand};
use log::Record;
use logforth::{Diagnostic, Layout, append, filter::EnvFilter};

mod produce;

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

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Options {
    /// The target topic name
    topic: String,

    /// The Pulsar service URL
    #[arg(long, default_value_t = String::from("pulsar://localhost:6650"))]
    service_url: String,

    /// The token to authenticate the target topic
    #[arg(long)]
    auth_token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Produce messages to the target topic
    Produce {
        // The number of messages to send per second
        #[arg(long, default_value_t = 500)]
        rate: u32,

        #[arg(short, long, default_value_t = 10000)]
        num_messages: u32,
        // TODO:
        //   1. support batching after batch timeout is supported
        //   2. support payload size or read payload from a file
    },
    // TODO: support Consume
}

fn main() {
    let options = Options::parse();

    println!("Topic: {}", options.topic);
    println!("Service URL: {}", options.service_url);

    logforth::builder()
        .dispatch(|d| {
            d.filter(EnvFilter::from_default_env())
                .append(append::Stdout::default().with_layout(CustomLayout))
        })
        .apply();

    match options.command {
        Commands::Produce { rate, num_messages } => {
            println!(
                "Produce {} messages with {} messages per second",
                num_messages, rate
            );
            produce::produce(
                options.topic,
                options.service_url,
                options.auth_token,
                rate,
                num_messages,
            );
        }
    }
}
