use ontodev_valve::cli;

#[async_std::main]
async fn main() {
    cli::process_command().await;
}
