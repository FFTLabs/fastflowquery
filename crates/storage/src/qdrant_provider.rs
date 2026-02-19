use std::collections::HashMap;

use ffq_common::{FfqError, Result};
use futures::future::{BoxFuture, FutureExt};
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{Condition, Filter, SearchPointsBuilder, Value, point_id};

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
            let parsed_filter = parse_filter_spec(filter)?;
            let mut req = SearchPointsBuilder::new(&self.collection, query_vec, k as u64)
                .with_payload(self.with_payload)
                .build();
            req.limit = k as u64;
            req.filter = parsed_filter;

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

#[derive(Debug, Clone, serde::Deserialize)]
struct QdrantFilterSpec {
    must: Vec<QdrantMatchClause>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct QdrantMatchClause {
    field: String,
    value: serde_json::Value,
}

fn parse_filter_spec(filter: Option<String>) -> Result<Option<Filter>> {
    let Some(raw) = filter else {
        return Ok(None);
    };
    let spec: QdrantFilterSpec = serde_json::from_str(&raw)
        .map_err(|e| FfqError::Execution(format!("invalid qdrant filter payload: {e}")))?;
    let mut conds = Vec::with_capacity(spec.must.len());
    for clause in spec.must {
        conds.push(condition_from_clause(clause)?);
    }
    Ok(Some(Filter::must(conds)))
}

fn condition_from_clause(clause: QdrantMatchClause) -> Result<Condition> {
    match clause.value {
        serde_json::Value::String(v) => Ok(Condition::matches(clause.field, v)),
        serde_json::Value::Bool(v) => Ok(Condition::matches(clause.field, v)),
        serde_json::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                Ok(Condition::matches(clause.field, i))
            } else {
                Err(FfqError::Unsupported(
                    "qdrant filter number must be i64".to_string(),
                ))
            }
        }
        other => Err(FfqError::Unsupported(format!(
            "unsupported qdrant filter literal type: {other}"
        ))),
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

#[cfg(test)]
mod tests {
    use super::parse_filter_spec;

    #[test]
    fn parses_compound_eq_filter_payload() {
        let raw = r#"{"must":[{"field":"tenant_id","value":42},{"field":"lang","value":"en"}]}"#;
        let parsed = parse_filter_spec(Some(raw.to_string())).expect("parse");
        let filter = parsed.expect("filter");
        assert_eq!(filter.must.len(), 2);
    }
}
