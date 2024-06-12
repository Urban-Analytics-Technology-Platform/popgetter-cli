use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    ops::{Index, IndexMut},
    str::FromStr,
};

use crate::{
    metadata::{Metadata, MetricId},
    parquet::MetricRequest,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct DataRequestSpec {
    pub geometry: GeometrySpec,
    pub region: Vec<RegionSpec>,
    pub metrics: Vec<MetricSpec>,
    pub years: Option<Vec<String>>,
}

pub struct MetricRequestResult {
    pub metrics: Vec<MetricRequest>,
    pub selected_geometry: String,
    pub years: Vec<String>,
}

impl DataRequestSpec {
    /// Generates a vector of metric requests from a `DataRequestSpec` and a catalogue.
    pub fn metric_requests(&self, catalogue: &Metadata) -> Result<MetricRequestResult> {
        // Find all the metrics which match the requested ones, expanding
        // any regex matches as we do so
        let expanded_metric_ids: Vec<MetricId> = self
            .metrics
            .iter()
            .filter_map(|metric_spec| match metric_spec {
                MetricSpec::Metric(id) => catalogue.expand_regex_metric(id).ok(),
                MetricSpec::DataProduct(_) => None,
            })
            .flatten()
            .collect::<Vec<_>>();

        let full_selection_plan =
            catalogue.generate_selection_plan(&expanded_metric_ids, &self.geometry, &self.years)?;

        println!("Running your query with \n {full_selection_plan}");

        let metric_requests =
            catalogue.get_metric_requests(full_selection_plan.explicit_metric_ids)?;

        Ok(MetricRequestResult {
            metrics: metric_requests,
            selected_geometry: full_selection_plan.geometry,
            years: full_selection_plan.year,
        })
    }
}

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

#[derive(Serialize, Deserialize, Debug)]
pub struct Polygon;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BBox(pub [f64; 4]);

impl Index<usize> for BBox {
    type Output = f64;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<usize> for BBox {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl FromStr for BBox {
    type Err = &'static str;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parts: Vec<f64> = value
            .split(',')
            .map(|s| s.trim().parse::<f64>().map_err(|_| "Failed to parse bbox"))
            .collect::<Result<Vec<_>, _>>()?;

        if parts.len() != 4 {
            return Err("Bounding boxes need to have 4 coords");
        }
        let mut bbox = [0.0; 4];
        bbox.copy_from_slice(&parts);
        Ok(BBox(bbox))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bbox_should_parse_if_correct() {
        let bbox = BBox::from_str("0.0,1.0,2.0,3.0");
        assert!(bbox.is_ok(), "A four coord bbox should parse");
    }

    #[test]
    fn bbox_should_not_parse_if_incorrect() {
        let bbox = BBox::from_str("0.0,1.0,2.0");
        assert!(
            bbox.is_err(),
            "A string with fewer than 4 coords should not parse"
        );
        let bbox = BBox::from_str("0.0,1.0,2.0,3.0,4.0");
        assert!(
            bbox.is_err(),
            "A string with 5 or more coords should not parse"
        );
        let bbox = BBox::from_str("0.0sdfsd,1.0,2.0");
        assert!(bbox.is_err(), "A string with letters shouldn't parse");
    }
}
