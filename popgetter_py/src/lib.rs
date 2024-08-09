use std::default::Default;

use ::popgetter::{
    data_request_spec::{DataRequestSpec, GeometrySpec, MetricSpec},
    search::{SearchParams, SearchText},
};
use polars::prelude::DataFrame;
use pyo3::{
    prelude::*,
    types::{PyDict, PyString},
};
use pyo3_polars::PyDataFrame;

async fn _search() -> DataFrame {
    let search_params: SearchParams = SearchParams::default();
    let popgetter = ::popgetter::Popgetter::new().await.unwrap();
    let search_results = popgetter.search(search_params);
    search_results
        .0
        .select([
            "metric_id",
            "human_readable_name",
            "metric_description",
            "metric_hxl_tag",
            "geometry_level",
        ])
        .unwrap()
}

// async fn _data()-> DataFrame{
//     let search_request: SearchRequest = SearchRequest::default();
//     let popgetter = Popgetter::new().await.unwrap();
//     popgetter.search(&search_request).await.unwrap()
// }

fn get_search(obj: &Bound<'_, PyAny>) -> PyResult<SearchParams> {
    if let Ok(text) = obj.downcast::<PyString>() {
        println!("object is a string {}", text);
        let search_text = SearchText {
            text: text.to_string(),
            ..SearchText::default()
        };
        return Ok(SearchParams {
            text: vec![search_text],
            ..SearchParams::default()
        });
    }

    if let Ok(dict) = obj.downcast::<PyDict>() {
        println!("Object is a dict {}", dict);
        return Ok(SearchParams::default());
    };

    Ok(SearchParams::default())
}

fn get_data_request(obj: &Bound<'_, PyAny>) -> PyResult<DataRequestSpec> {
    Ok(DataRequestSpec {
        geometry: Some(GeometrySpec {
            include_geoms: true,
            geometry_level: None,
        }),
        region: vec![],
        metrics: vec![MetricSpec::MetricText(r"\#population\+adults".to_string())],
        years: None,
    })
}

async fn _get_data(data_request: &DataRequestSpec) -> DataFrame {
    let popgetter = ::popgetter::Popgetter::new().await.unwrap();
    println!("running data request {:#?}", data_request);
    todo!("Uncomment the below when get_data_request implemented")
    // popgetter.get_data_request(data_request).await.unwrap()
}

#[pyfunction]
fn get(
    #[pyo3(from_py_with = "get_data_request")] data_request: DataRequestSpec,
) -> PyResult<PyDataFrame> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // Call the asynchronous connect method using the runtime.
    let result = rt.block_on(_get_data(&data_request));
    Ok(PyDataFrame(result))
}

#[pyfunction]
fn search(
    #[pyo3(from_py_with = "get_search")] search_query: SearchParams,
) -> PyResult<PyDataFrame> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // Call the asynchronous connect method using the runtime.
    let result = rt.block_on(_search());
    Ok(PyDataFrame(result))
}

/// A Python module implemented in Rust.
#[pymodule]
fn popgetter(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(search, m)?)?;
    m.add_function(wrap_pyfunction!(get, m)?)?;
    Ok(())
}
