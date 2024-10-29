// To run this example execute: cargo run --example vector_store_qdrant --features qdrant

use std::collections::HashMap;

use anyhow::anyhow;
use itertools::izip;
use langchain_rust::{
    schemas::Document,
    vectorstore::{qdrant::Store, VecStoreOptions, VectorStore},
};
use popgetter::{Popgetter, COL};
use serde_json::Value;

pub async fn init_embeddings(
    // client: &Qdrant,
    // embedder: OpenAiEmbedder<AzureConfig>,
    store: &mut Store,
) -> anyhow::Result<()> {
    // Initialize Embedder
    // let embedder = azure_open_ai_embedding(&api_key()?);

    // // Initialize the qdrant_client::Qdrant
    // // Ensure Qdrant is running at localhost, with gRPC port at 6334
    // // docker run -p 6334:6334 qdrant/qdrant
    // let client = Qdrant::from_url("http://localhost:6334").build().unwrap();

    // let store = StoreBuilder::new()
    //     .embedder(embedder)
    //     .client(client)
    //     .collection_name("popgetter_1000_400")
    //     .build()
    //     .await
    //     .unwrap();

    let popgetter = Popgetter::new_with_config_and_cache(Default::default()).await?;
    let combined_metadata = popgetter
        .metadata
        .combined_metric_source_geometry()
        .0
        .collect()?;
    let mut v = vec![];

    for (description, country, id) in izip!(
        combined_metadata
            .column(COL::METRIC_HUMAN_READABLE_NAME)?
            .str()?
            .into_iter(),
        combined_metadata
            .column(COL::COUNTRY_NAME_SHORT_EN)?
            .str()?
            .into_iter(),
        combined_metadata.column(COL::METRIC_ID)?.str()?.into_iter()
    ) {
        let s: String = description.ok_or(anyhow!("Not a str"))?.into();

        // TODO: add method to return HashMap of a row with keys (columns) and values
        // Could just use the IDs and lookup in polars too.
        let mut hm: HashMap<String, Value> = HashMap::new();
        hm.insert(
            "country".to_owned(),
            Value::String(country.unwrap().to_string()),
        );
        hm.insert(
            COL::METRIC_ID.to_owned(),
            Value::String(id.unwrap().to_string()),
        );

        // TODO: add other metadata
        let doc = Document::new(s).with_metadata(hm);
        v.push(doc);
    }

    // Add documents to store
    store
        .add_documents(&v, &VecStoreOptions::default())
        .await
        // TODO: update error to not convert to string
        .map_err(|err| anyhow!(err.to_string()))?;

    Ok(())
}

pub async fn query_embeddings(
    query: &str,
    limit: usize,
    store: &Store,
) -> anyhow::Result<Vec<Document>> {
    // TODO: see if we can subset similarity search by metadata values
    let results = store
        .similarity_search(query, limit, &VecStoreOptions::default())
        .await
        // TODO: fix error type
        .unwrap();

    // TODO: Add filtering by metadata values (e.g. country)
    // https://qdrant.tech/documentation/concepts/hybrid-queries/?q=color#re-ranking-with-payload-values
    if results.is_empty() {
        println!("No results found.");
        return Ok(results);
    } else {
        results.iter().for_each(|r| {
            println!("Document: {:#?}", r);
        });
    }
    Ok(results)
}
