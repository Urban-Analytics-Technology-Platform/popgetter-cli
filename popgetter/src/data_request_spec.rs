// TODO: this module to be refactored following implementation of SearchParams.
// See [#67](https://github.com/Urban-Analytics-Technology-Platform/popgetter-cli/issues/67)

use itertools::Itertools;
use nonempty::nonempty;
use serde::{Deserialize, Serialize};

use crate::geo::BBox;
use crate::search::{GeometryLevel, MetricId, SearchContext, SearchParams, SearchText, YearRange};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct DataRequestSpec {
    pub geometry: Option<GeometrySpec>,
    pub region: Vec<RegionSpec>,
    pub metrics: Vec<MetricSpec>,
    pub years: Option<Vec<String>>,
}

impl TryFrom<DataRequestSpec> for SearchParams {
    type Error = anyhow::Error;
    fn try_from(value: DataRequestSpec) -> Result<Self, Self::Error> {
        // TODO: handle MetricSpec::DataProduct variant
        Ok(Self {
            // TODO: consider updating for regex field following [#66](https://github.com/Urban-Analytics-Technology-Platform/popgetter-cli/issues/66)
            text: value
                .metrics
                .iter()
                .filter_map(|metric| match metric {
                    MetricSpec::MetricText(text) => Some(SearchText {
                        text: text.clone(),
                        context: nonempty![
                            SearchContext::HumanReadableName,
                            SearchContext::Hxl,
                            SearchContext::Description
                        ],
                    }),
                    _ => None,
                })
                .collect_vec(),
            year_range: if let Some(v) = value.years {
                Some(
                    v.iter()
                        .map(|year| year.parse::<YearRange>())
                        .collect::<Result<Vec<_>, anyhow::Error>>()?,
                )
            } else {
                None
            },
            metric_id: value
                .metrics
                .iter()
                .filter_map(|metric| match metric {
                    MetricSpec::MetricId(m) => Some(m.clone()),
                    _ => None,
                })
                .collect_vec(),
            geometry_level: value
                .geometry
                .and_then(|geometry| geometry.geometry_level.map(GeometryLevel)),
            source_data_release: None,
            data_publisher: None,
            country: None,
            source_metric_id: None,
            region_spec: value.region,
        })
    }
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
    MetricId(MetricId),
    MetricText(String),
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

#[derive(Clone, Serialize, Deserialize, Debug)]
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Polygon;

#[cfg(test)]
mod tests {}
