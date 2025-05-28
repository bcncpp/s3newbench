use aws_sdk_s3::{Client as S3Client, Error as S3Error};
use aws_sdk_s3::types::ByteStream;
use aws_types::credentials::Credentials;
use aws_config::meta::region::RegionProviderChain;
use clap::Parser;
use elasticsearch::{Elasticsearch, http::transport::Transport};
use uuid::Uuid;
use chrono::Utc;
use rand::seq::SliceRandom;
use std::time::{Instant, Duration};
use std::error::Error;

#[derive(Parser, Debug)]
#[clap(author="Shon Paz", version="1.0", about="Interactive benchmark tool for S3 operations")]
struct Args {
    #[clap(short = 'e', long, help = "Endpoint URL for S3 object storage")]
    endpoint_url: String,

    #[clap(short = 'a', long, help = "Access key for S3 object storage")]
    access_key: String,

    #[clap(short = 's', long, help = "Secret key for S3 object storage")]
    secret_key: String,

    #[clap(short = 'b', long, help = "S3 bucket name")]
    bucket_name: String,

    #[clap(short = 'o', long, help = "S3 object size (e.g. 10MB)")]
    object_size: String,

    #[clap(short = 'u', long, help = "Elasticsearch cluster URL")]
    elastic_url: String,

    #[clap(short = 'n', long, help = "Number of objects to put/get")]
    num_objects: usize,

    #[clap(short = 'w', long, help = "Workload running on S3 - read/write")]
    workload: String,

    #[clap(short = 'l', long, help = "Max acceptable latency per object operation in ms")]
    max_latency: Option<f64>,

    #[clap(short = 'p', long, help = "A prefix (directory) located in the bucket")]
    prefix: Option<String>,

    #[clap(short = 'c', long, help = "Should we cleanup all the objects written? yes/no")]
    cleanup: Option<String>,
}

struct ObjectAnalyzer {
    s3: S3Client,
    elastic: Elasticsearch,
    args: Args,
    cleanup_list: Vec<String>,
}

impl ObjectAnalyzer {
    async fn new(args: Args) -> Result<Self, Box<dyn Error>> {
        // Setup AWS config
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let shared_config = aws_config::from_env()
            .region(region_provider)
            .credentials_provider(Credentials::new(
                &args.access_key,
                &args.secret_key,
                None,
                None,
                "custom",
            ))
            .load()
            .await;

        let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config)
            .endpoint_url(&args.endpoint_url);
        let s3 = S3Client::from_conf(s3_config_builder.build());

        // Elasticsearch client setup
        let transport = Transport::single_node(&args.elastic_url)?;
        let elastic = Elasticsearch::new(transport);

        Ok(Self {
            s3,
            elastic,
            args,
            cleanup_list: Vec::new(),
        })
    }

    async fn check_bucket_existence(&self) -> bool {
        self.s3.head_bucket()
            .bucket(&self.args.bucket_name)
            .send()
            .await
            .is_ok()
    }

    async fn create_bucket(&self) -> Result<(), S3Error> {
        self.s3.create_bucket()
            .bucket(&self.args.bucket_name)
            .send()
            .await?;
        Ok(())
    }

    fn calculate_throughput(latency_ms: f64, size_bytes: usize) -> f64 {
        // throughput = (1000 / latency_ms) * size_bytes / (1000^2)
        (1000.0 / latency_ms) * (size_bytes as f64) / 1_000_000.0
    }

    fn generate_object_name(&self) -> String {
        if let Some(prefix) = &self.args.prefix {
            format!("{}/{}", prefix, Uuid::new_v4())
        } else {
            Uuid::new_v4().to_string()
        }
    }

    fn create_bin_data(&self) -> Vec<u8> {
        // Simple parse size (supports K, M, G suffixes)
        fn parse_size(size_str: &str) -> usize {
            let size_str = size_str.trim().to_uppercase();
            if size_str.ends_with("KB") {
                size_str[..size_str.len()-2].parse::<usize>().unwrap_or(0) * 1024
            } else if size_str.ends_with("MB") {
                size_str[..size_str.len()-2].parse::<usize>().unwrap_or(0) * 1024 * 1024
            } else if size_str.ends_with("GB") {
                size_str[..size_str.len()-2].parse::<usize>().unwrap_or(0) * 1024 * 1024 * 1024
            } else {
                size_str.parse::<usize>().unwrap_or(0)
            }
        }

        let size = parse_size(&self.args.object_size);
        vec![b'a'; size]
    }

    async fn put_object(&mut self, object_name: &str, bin_data: &[u8]) -> Result<(), S3Error> {
        self.s3.put_object()
            .bucket(&self.args.bucket_name)
            .key(object_name)
            .body(ByteStream::from(bin_data.to_vec()))
            .send()
            .await?;
        self.cleanup_list.push(object_name.to_string());
        Ok(())
    }

    async fn get_object(&self, object_name: &str) -> Result<(), S3Error> {
        let resp = self.s3.get_object()
            .bucket(&self.args.bucket_name)
            .key(object_name)
            .send()
            .await?;

        let data = resp.body.collect().await?;
        // We can do something with data if needed
        Ok(())
    }

    fn evaluate_latency(&self, duration_ms: f64) -> bool {
        if let Some(max) = self.args.max_latency {
            duration_ms > max
        } else {
            false
        }
    }

    fn create_timestamp() -> i64 {
        Utc::now().timestamp_millis()
    }

    async fn write_elastic_data(&self, data: serde_json::Value) -> Result<(), Box<dyn Error>> {
        self.elastic.index(elasticsearch::IndexParts::Index("s3-perf-index"))
            .body(data)
            .send()
            .await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        // Check bucket and create if needed
        let exists = self.check_bucket_existence().await;
        if !exists && self.args.workload.to_lowercase() == "write" {
            self.create_bucket().await?;
        }

        let data = self.create_bin_data();

        let source = format!("{}{}", hostname::get()?.to_string_lossy(), Uuid::new_v4());

        if self.args.workload.to_lowercase() == "write" {
            for _ in 0..self.args.num_objects {
                let object_name = self.generate_object_name();

                let start = Instant::now();
                self.put_object(&object_name, &data).await?;
                let duration = start.elapsed();
                let duration_ms = duration.as_secs_f64() * 1000.0;

                let exceeded = self.evaluate_latency(duration_ms);

                let size_bytes = data.len();

                let throughput = Self::calculate_throughput(duration_ms, size_bytes);

                let doc = serde_json::json!({
                    "latency": duration_ms,
                    "latency_exceeded": exceeded,
                    "timestamp": Self::create_timestamp(),
                    "workload": self.args.workload,
                    "size": self.args.object_size,
                    "size_in_bytes": size_bytes,
                    "throughput": throughput,
                    "object_name": object_name,
                    "source": source,
                });
                self.write_elastic_data(doc).await?;
            }
        } else if self.args.workload.to_lowercase() == "read" {
            // TODO: implement list_random_objects method and shuffle/pagination as in Python
            // For now, just print warning:
            println!("Read workload not implemented yet");
        }

        if let Some(cleanup) = &self.args.cleanup {
            if cleanup.to_lowercase() == "yes" {
                for key in &self.cleanup_list {
                    self.s3.delete_object()
                        .bucket(&self.args.bucket_name)
                        .key(key)
                        .send()
                        .await?;
                }
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut analyzer = ObjectAnalyzer::new(args).await?;
    analyzer.run().await?;

    Ok(())
}
