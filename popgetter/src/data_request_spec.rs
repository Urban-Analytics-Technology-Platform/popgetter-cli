use itertools::Itertools;
use nonempty::nonempty;
use serde::{Deserialize, Serialize};

use crate::geo::BBox;
use crate::search::{
    CaseSensitivity, DownloadParams, GeometryLevel, MatchType, MetricId, Params, SearchConfig,
    SearchContext, SearchParams, SearchText, YearRange,
};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct DataRequestSpec {
    pub geometry: Option<GeometrySpec>,
    pub region: Vec<RegionSpec>,
    pub metrics: Vec<MetricSpec>,
    pub years: Option<Vec<String>>,
}

// Since `DataRequestSpec` contains parameters relevant to both `SearchParams` and `DownloadParams`,
// the conversion is implemented for `Params`.
impl TryFrom<DataRequestSpec> for Params {
    type Error = anyhow::Error;
    fn try_from(value: DataRequestSpec) -> Result<Self, Self::Error> {
        // TODO: handle MetricSpec::DataProduct variant
        Ok(Self {
            search: SearchParams {
                // TODO: consider defaults during implementing [#81](https://github.com/Urban-Analytics-Technology-Platform/popgetter-cli/issues/81)
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
                            config: SearchConfig {
                                match_type: MatchType::Regex,
                                case_sensitivity: CaseSensitivity::Insensitive,
                            },
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
                geometry_level: value.geometry.as_ref().and_then(|geometry| {
                    geometry
                        .geometry_level
                        .to_owned()
                        .map(|geometry_level| GeometryLevel {
                            value: geometry_level,
                            config: SearchConfig {
                                match_type: MatchType::Exact,
                                case_sensitivity: CaseSensitivity::Insensitive,
                            },
                        })
                }),
                source_data_release: None,
                source_download_url: None,
                data_publisher: None,
                country: None,
                source_metric_id: None,
                region_spec: value.region.clone(),
            },
            download: DownloadParams {
                include_geoms: value.geometry.unwrap_or_default().include_geoms,
                region_spec: value.region,
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum MetricSpec {
    MetricId(MetricId),
    MetricText(String),
    DataProduct(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
