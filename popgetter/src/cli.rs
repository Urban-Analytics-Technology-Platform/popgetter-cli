// FromStr is required by EnumString. The compiler seems to not be able to
// see that and so is giving a warning. Dont remove it
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use enum_dispatch::enum_dispatch;
use log::{debug, info};
use popgetter::{
    config::Config,
    data_request_spec::{BBox, DataRequestSpec, GeometrySpec, MetricSpec, RegionSpec},
    formatters::{
        CSVFormatter, GeoJSONFormatter, GeoJSONSeqFormatter, OutputFormatter, OutputGenerator,
    },
    metadata::MetricId,
    search::*,
    Popgetter,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use strum_macros::EnumString;

use crate::display::display_search_results;

/// Defines the output formats we are able to produce data in.
#[derive(Clone, Debug, Deserialize, Serialize, EnumString, PartialEq, Eq)]
#[strum(ascii_case_insensitive)]
pub enum OutputFormat {
    GeoJSON,
    GeoJSONSeq,
    Csv,
    GeoParquet,
    FlatGeobuf,
}

/// Trait that defines what to run when a given subcommand is invoked.
#[enum_dispatch]
pub trait RunCommand {
    async fn run(&self, config: Config) -> Result<()>;
}

/// The Data command is the one we invoke to get a set of metrics and geometry
/// for some given region and set of metrics. Currently it takes two arguments
/// - bbox: A Bounding box
/// - metrics: A comma seperated list of metrics to retrive.
///
/// The Data command can be converted into a `DataRequestSpec` which is the processed
/// by the core library.
#[derive(Args, Debug)]
pub struct DataCommand {
    /// Only get data in  bounding box ([min_lat,min_lng,max_lat,max_lng])
    #[arg(
        short,
        long,
        allow_hyphen_values(true),
        help = "Bounding box in which to get the results. Format is: min_lon, min_lat, max_lon, max_lat "
    )]
    bbox: Option<BBox>,
    /// Specify a metric by hxl
    #[arg(
        short = 'h',
        long,
        help = "Specify a metric by Humanitarian Exchange Language tag"
    )]
    hxl: Option<Vec<String>>,

    // Specify a metric by id
    #[arg(
        short = 'i',
        long,
        help = "Specify a metric by uuid, can be a partial uuid"
    )]
    id: Option<Vec<String>>,

    // Specify a metric by name
    #[arg(short = 'n', long, help = "Specify a metric by Human readable name")]
    name: Option<Vec<String>>,

    /// Specify output format
    #[arg(short = 'f', long, help = "One of GeoJSON, CSV, GeoJSONSeq")]
    output_format: OutputFormat,

    /// Specify where the result should be saved
    #[arg(short = 'o', long, help = "Output file to place the results")]
    output_file: String,

    /// Specify the years we should get the result for
    #[arg(
        short = 'y',
        long,
        help = "Specify the year ranges for which you are interested in the metrics"
    )]
    years: Option<Vec<String>>,
}

impl DataCommand {
    pub fn gather_metric_requests(&self) -> Vec<MetricId> {
        let mut metric_ids: Vec<MetricId> = vec![];

        if let Some(ids) = &self.id {
            for id in ids {
                metric_ids.push(MetricId::Id(id.clone()));
            }
        }

        if let Some(hxls) = &self.hxl {
            for hxl in hxls {
                metric_ids.push(MetricId::Hxl(hxl.clone()));
            }
        }

        if let Some(names) = &self.name {
            for name in names {
                metric_ids.push(MetricId::CommonName(name.clone()));
            }
        }

        metric_ids
    }
}

impl From<&OutputFormat> for OutputFormatter {
    fn from(value: &OutputFormat) -> Self {
        match value {
            OutputFormat::GeoJSON => OutputFormatter::GeoJSON(GeoJSONFormatter),
            OutputFormat::Csv => OutputFormatter::Csv(CSVFormatter::default()),
            OutputFormat::GeoJSONSeq => OutputFormatter::GeoJSONSeq(GeoJSONSeqFormatter),
            _ => todo!("output format not implemented"),
        }
    }
}

impl From<OutputFormat> for OutputFormatter {
    fn from(value: OutputFormat) -> Self {
        Self::from(&value)
    }
}

impl RunCommand for DataCommand {
    async fn run(&self, config: Config) -> Result<()> {
        info!("Running `data` subcommand");

        let popgetter = Popgetter::new_with_config(config).await?;
        let data_request = DataRequestSpec::from(self);
        let mut results = popgetter.get_data_request(&data_request).await?;

        debug!("{results:#?}");
        let mut f = File::create(&self.output_file)?;
        let formatter: OutputFormatter = (&self.output_format).into();
        formatter.save(&mut f, &mut results)?;

        Ok(())
    }
}

impl From<&DataCommand> for DataRequestSpec {
    fn from(value: &DataCommand) -> Self {
        let region = if let Some(bbox) = value.bbox.clone() {
            vec![RegionSpec::BoundingBox(bbox)]
        } else {
            vec![]
        };

        let metrics = value
            .gather_metric_requests()
            .into_iter()
            .map(MetricSpec::Metric)
            .collect();

        DataRequestSpec {
            geometry: GeometrySpec::default(),
            region,
            metrics,
            years: None,
        }
    }
}

/// The Metrics command allows a user to search for a set of metrics by bounding box and filter.
/// The set of ways to search will likley increase over time
#[derive(Args, Debug)]
pub struct MetricsCommand {
    #[arg(
        short,
        long,
        value_name = "min_lat,min_lng,max_lat,max_lng",
        help = "Bounding box in which to get the results"
    )]
    bbox: Option<BBox>,
    #[arg(short, long, help = "Filter by year")]
    year: Option<Vec<String>>,
    #[arg(short, long, help = "Filter by geometry level")]
    geometry_level: Option<Vec<String>>,
    #[arg(short, long, help = "Filter by source data release name")]
    source_data_release: Option<Vec<String>>,
    #[arg(short, long, help = "Filter by data publisher name")]
    publisher: Option<Vec<String>>,
    #[arg(short, long, help = "Filter by country")]
    country: Option<Vec<String>>,
    #[arg(
        long,
        help = "Filter by source metric ID (i.e. the name of the table in the original data release)"
    )]
    source_metric_id: Option<Vec<String>>,
    // Filters for text
    #[arg(long, help="Filter by HXL tag", num_args=0..)]
    hxl: Vec<String>,
    #[arg(long, help="Filter by metric name", num_args=0..)]
    name: Vec<String>,
    #[arg(long, help="Filter by metric description", num_args=0..)]
    description: Vec<String>,
    #[arg(short, long, help="Filter by HXL tag, name, or description", num_args=0..)]
    text: Vec<String>,
    // Output options
    #[arg(
        short,
        long,
        help = "Show all metrics even if there are a large number"
    )]
    full: bool,
}

impl RunCommand for MetricsCommand {
    async fn run(&self, config: Config) -> Result<()> {
        info!("Running `metrics` subcommand");
        debug!("{:#?}", self);

        let mut all_text_searches: Vec<SearchText> = vec![];
        all_text_searches.extend(self.hxl.iter().map(|t| SearchText {
            text: t.clone(),
            context: vec![SearchContext::Hxl],
        }));
        all_text_searches.extend(self.name.iter().map(|t| SearchText {
            text: t.clone(),
            context: vec![SearchContext::HumanReadableName],
        }));
        all_text_searches.extend(self.description.iter().map(|t| SearchText {
            text: t.clone(),
            context: vec![SearchContext::Description],
        }));
        all_text_searches.extend(self.text.iter().map(|t| SearchText {
            text: t.clone(),
            context: SearchContext::all(),
        }));

        let search_request = SearchRequest {
            text: all_text_searches,
            year: self.year.clone().map(Year),
            geometry_level: self.geometry_level.clone().map(GeometryLevel),
            source_data_release: self.source_data_release.clone().map(SourceDataRelease),
            data_publisher: self.publisher.clone().map(DataPublisher),
            country: self.country.clone().map(Country),
            census_table: self.source_metric_id.clone().map(SourceMetricId),
        };
        let popgetter = Popgetter::new_with_config(config).await?;

        let search_results = popgetter.search(&search_request).await?;
        let len_requests = search_results.0.shape().0;

        println!("Found {} metrics.", len_requests);

        if len_requests > 50 && !self.full {
            display_search_results(search_results, Some(50));
            println!(
                "{} more results not shown. Use --full to show all results.",
                len_requests - 50
            );
        } else {
            display_search_results(search_results, None);
        }
        Ok(())
    }
}

/// The Countries command should return information about the various countries we have data for.
/// This could include metrics like the number of surveys / metrics / geographies avaliable for each country.
#[derive(Args, Debug)]
pub struct CountriesCommand;

impl RunCommand for CountriesCommand {
    async fn run(&self, config: Config) -> Result<()> {
        let _popgetter = Popgetter::new_with_config(config).await?;
        Ok(())
    }
}

/// The Surveys command should list the various surveys that popgetter has access to and releated
/// stastistics.
#[derive(Args, Debug)]
pub struct SurveysCommand;

impl RunCommand for SurveysCommand {
    async fn run(&self, config: Config) -> Result<()> {
        info!("Running `surveys` subcommand");
        Ok(())
    }
}

/// The Recipe command loads a recipy file and generates the output data requested
#[derive(Args, Debug)]
pub struct RecipeCommand {
    #[arg(index = 1)]
    recipe_file: String,

    #[arg(short = 'f', long)]
    output_format: OutputFormat,

    #[arg(short = 'o', long)]
    output_file: String,
}

impl RunCommand for RecipeCommand {
    async fn run(&self, config: Config) -> Result<()> {
        let popgetter = Popgetter::new_with_config(config).await?;
        let config = fs::read_to_string(&self.recipe_file)?;
        let data_request: DataRequestSpec = serde_json::from_str(&config)?;
        let mut results = popgetter.get_data_request(&data_request).await?;
        println!("{results}");
        let formatter: OutputFormatter = (&self.output_format).into();
        let mut f = File::create(&self.output_file)?;
        formatter.save(&mut f, &mut results)?;
        Ok(())
    }
}

/// The entrypoint for the CLI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None, name="popgetter", long_about="Popgetter is a tool to quickly get the data you need!")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Commands contains the list of subcommands avaliable for use in the CLI.
/// Each command should implmement the RunCommand trait and specify the list
/// of required args for that command.
#[derive(Subcommand, Debug)]
#[enum_dispatch(RunCommand)]
pub enum Commands {
    /// List countries for which data are available
    Countries(CountriesCommand),
    /// Output data for a given region and set of metrics
    Data(DataCommand),
    /// List and filter available metrics. Multiple filters are applied conjunctively, i.e. this
    /// command only returns metrics that match all filters.
    Metrics(MetricsCommand),
    /// Surveys
    Surveys(SurveysCommand),
    /// From recipe
    Recipe(RecipeCommand),
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn output_type_should_deserialize_properly() {
        let output_format = OutputFormat::from_str("GeoJSON");
        assert_eq!(
            output_format.unwrap(),
            OutputFormat::GeoJSON,
            "geojson format should be parsed correctly"
        );
        let output_format = OutputFormat::from_str("GeoJson");
        assert_eq!(
            output_format.unwrap(),
            OutputFormat::GeoJSON,
            "parsing should be case insensitive"
        );
        let output_format = OutputFormat::from_str("geoparquet");
        assert_eq!(
            output_format.unwrap(),
            OutputFormat::GeoParquet,
            "correct variants should parse correctly"
        );
        let output_format = OutputFormat::from_str("awesome_tiny_model");
        assert!(output_format.is_err(), "non listed formats should fail");
    }
}
