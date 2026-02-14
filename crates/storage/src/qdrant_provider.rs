use std::collections::HashMap;

use ffq_common::{FfqError, Result};
use futures::future::{BoxFuture, FutureExt};
use qdrant_client::qdrant::{point_id, SearchPointsBuilder, Value};
use qdrant_client::Qdrant;

use crate::vector_index::{VectorIndexProvider, VectorTopKRow};

#[derive(Clone)]
pub struct QdrantProvider {
    client: Qdrant,
    collection: String,
    with_payload: bool,
}

impl std::fmt::Debug for QdrantProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantProvider")
            .field("collection", &self.collection)
            .field("with_payload", &self.with_payload)
            .finish()
    }
}

impl QdrantProvider {
    pub fn from_table(table: &crate::TableDef) -> Result<Self> {
        let endpoint = table
            .options
            .get("qdrant.endpoint")
            .cloned()
            .unwrap_or_else(|| "http://127.0.0.1:6334".to_string());
        let collection = table
            .options
            .get("qdrant.collection")
            .cloned()
            .or_else(|| (!table.uri.is_empty()).then_some(table.uri.clone()))
            .unwrap_or_else(|| table.name.clone());
        let with_payload = table
            .options
            .get("qdrant.with_payload")
            .is_some_and(|v| v == "true" || v == "1");

        let client = Qdrant::from_url(&endpoint).build().map_err(|e| {
            FfqError::Execution(format!("qdrant client init failed for {endpoint}: {e}"))
        })?;
        Ok(Self {
            client,
            collection,
            with_payload,
        })
    }
}

impl VectorIndexProvider for QdrantProvider {
    fn topk<'a>(
        &'a self,
        query_vec: Vec<f32>,
        k: usize,
        filter: Option<String>,
    ) -> BoxFuture<'a, Result<Vec<VectorTopKRow>>> {
        async move {
            if filter.is_some() {
                return Err(FfqError::Unsupported(
                    "qdrant filter string is not implemented yet in v1".to_string(),
                ));
            }
            let mut req = SearchPointsBuilder::new(&self.collection, query_vec, k as u64)
                .with_payload(self.with_payload)
                .build();
            req.limit = k as u64;

            let response = self.client.search_points(req).await.map_err(|e| {
                FfqError::Execution(format!(
                    "qdrant search_points failed for collection '{}': {e}",
                    self.collection
                ))
            })?;

            let mut out = Vec::with_capacity(response.result.len());
            for point in response.result {
                let id = point
                    .id
                    .and_then(|p| p.point_id_options)
                    .ok_or_else(|| FfqError::Execution("qdrant point missing id".to_string()))
                    .and_then(|id_opt| match id_opt {
                        point_id::PointIdOptions::Num(n) => Ok(n as i64),
                        point_id::PointIdOptions::Uuid(u) => Err(FfqError::Unsupported(format!(
                            "qdrant UUID ids are not supported in v1: {u}"
                        ))),
                    })?;

                let payload_json = if self.with_payload {
                    Some(
                        serde_json::to_string(&payload_to_json(point.payload)).map_err(|e| {
                            FfqError::Execution(format!("qdrant payload json encode failed: {e}"))
                        })?,
                    )
                } else {
                    None
                };

                out.push(VectorTopKRow {
                    id,
                    score: point.score,
                    payload_json,
                });
            }
            Ok(out)
        }
        .boxed()
    }
}

fn payload_to_json(payload: HashMap<String, Value>) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(payload.len());
    for (k, v) in payload {
        map.insert(k, value_to_json(v));
    }
    serde_json::Value::Object(map)
}

fn value_to_json(v: Value) -> serde_json::Value {
    match v.kind {
        Some(qdrant_client::qdrant::value::Kind::NullValue(_)) => serde_json::Value::Null,
        Some(qdrant_client::qdrant::value::Kind::DoubleValue(x)) => serde_json::Value::from(x),
        Some(qdrant_client::qdrant::value::Kind::IntegerValue(x)) => serde_json::Value::from(x),
        Some(qdrant_client::qdrant::value::Kind::StringValue(x)) => serde_json::Value::from(x),
        Some(qdrant_client::qdrant::value::Kind::BoolValue(x)) => serde_json::Value::from(x),
        Some(qdrant_client::qdrant::value::Kind::StructValue(s)) => {
            let mut map = serde_json::Map::with_capacity(s.fields.len());
            for (k, v) in s.fields {
                map.insert(k, value_to_json(v));
            }
            serde_json::Value::Object(map)
        }
        Some(qdrant_client::qdrant::value::Kind::ListValue(list)) => {
            serde_json::Value::Array(list.values.into_iter().map(value_to_json).collect())
        }
        None => serde_json::Value::Null,
    }
}
