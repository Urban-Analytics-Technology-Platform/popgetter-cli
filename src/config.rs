use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(default)]
pub struct Config {
    pub base_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            // TODO: add fn to generate the release directory name from the CLI version directly
            // E.g. this could be achieved with: https://docs.rs/built/latest/built/
            base_path: "https://popgetter.blob.core.windows.net/releases/v0.2".into(),
        }
    }
}
