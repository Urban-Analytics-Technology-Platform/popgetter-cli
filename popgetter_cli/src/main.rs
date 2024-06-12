mod cli;
mod display;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, RunCommand};
use log::debug;
use popgetter::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();

    let args = Cli::parse();
    debug!("args: {args:?}");
    let config: Config = read_config_from_toml();
    debug!("config: {config:?}");

    if let Some(command) = args.command {
        command.run(config).await?;
    }
    Ok(())
}

fn read_config_from_toml() -> Config {
    // macOS: ~/.config/popgetter/config.toml
    let xdg_dirs = xdg::BaseDirectories::with_prefix("popgetter").unwrap();
    let file_path = xdg_dirs.place_config_file("config.toml").unwrap();

    match std::fs::read_to_string(file_path) {
        Ok(contents) => toml::from_str(&contents).expect("Invalid TOML in config file"),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                Config::default()
            } else {
                panic!("Error reading config file: {:#?}", e);
            }
        }
    }
}
