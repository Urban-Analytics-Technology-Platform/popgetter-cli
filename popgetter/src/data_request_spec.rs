// TODO: this module to be refactored following implementation of SearchParams.
// See [#67](https://github.com/Urban-Analytics-Technology-Platform/popgetter-cli/issues/67)

use serde::{Deserialize, Serialize};

use crate::geo::BBox;
use crate::search::MetricId;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct DataRequestSpec {
    pub geometry: GeometrySpec,
    pub region: Vec<RegionSpec>,
    pub metrics: Vec<MetricSpec>,
    pub years: Option<Vec<String>>,
}

// #[derive(Debug)]
// pub struct MetricRequestResult {
//     pub metrics: Vec<MetricRequest>,
//     pub selected_geometry: String,
//     pub years: Vec<String>,
// }
//
// impl DataRequestSpec {
//     /// Generates a vector of metric requests from a `DataRequestSpec` and a catalogue.
//     pub fn metric_requests(
//         &self,
//         catalogue: &Metadata,
//         config: &Config,
//     ) -> Result<MetricRequestResult> {
//         // Find all the metrics which match the requested ones, expanding
//         // any regex matches as we do so
//         let expanded_metric_ids: Vec<MetricId> = self
//             .metrics
//             .iter()
//             .filter_map(|metric_spec| match metric_spec {
//                 MetricSpec::Metric(id) => catalogue.expand_regex_metric(id).ok(),
//                 MetricSpec::DataProduct(_) => None,
//             })
//             .flatten()
//             .collect::<Vec<_>>();

//         let full_selection_plan =
//             catalogue.generate_selection_plan(&expanded_metric_ids, &self.geometry, &self.years)?;

//         info!("Running your query with \n {full_selection_plan}");

//         let metric_requests =
//             catalogue.get_metric_requests(full_selection_plan.explicit_metric_ids, config)?;

//         Ok(MetricRequestResult {
//             metrics: metric_requests,
//             selected_geometry: full_selection_plan.geometry,
//             years: full_selection_plan.year,
//         })
//     }
// }

#[derive(Serialize, Deserialize, Debug)]
pub enum MetricSpec {
    Metric(MetricId),
    DataProduct(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeometrySpec {
    pub geometry_level: Option<String>,
    pub include_geoms: bool,
}

impl Default for GeometrySpec {
    fn default() -> Self {
        Self {
            include_geoms: true,
            geometry_level: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RegionSpec {
    BoundingBox(BBox),
    Polygon(Polygon),
    NamedArea(String),
}

impl RegionSpec {
    pub fn bbox(&self) -> Option<BBox> {
        match self {
            RegionSpec::BoundingBox(bbox) => Some(bbox.clone()),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Polygon;

#[cfg(test)]
mod tests {}
