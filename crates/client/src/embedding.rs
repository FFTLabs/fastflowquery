use ffq_common::{FfqError, Result};

/// Stable embedding provider contract used by hybrid/vector workflows.
///
/// Implementors may call local models, external services, or custom pipelines.
pub trait EmbeddingProvider: Send + Sync {
    /// Embeds `texts` into dense vectors.
    ///
    /// Implementations must return exactly one vector per input text.
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
}

impl<F> EmbeddingProvider for F
where
    F: Fn(&[String]) -> Result<Vec<Vec<f32>>> + Send + Sync,
{
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        self(texts)
    }
}

/// Deterministic sample embedding provider for tests/examples.
///
/// This is not semantically meaningful embedding quality; it is intended only
/// for wiring and integration validation.
#[derive(Debug, Clone)]
pub struct SampleEmbeddingProvider {
    dim: usize,
}

impl SampleEmbeddingProvider {
    /// Creates a sample provider with fixed output dimension.
    pub fn new(dim: usize) -> Result<Self> {
        if dim == 0 {
            return Err(FfqError::InvalidConfig(
                "sample embedding dimension must be > 0".to_string(),
            ));
        }
        Ok(Self { dim })
    }
}

impl EmbeddingProvider for SampleEmbeddingProvider {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut out = Vec::with_capacity(texts.len());
        for text in texts {
            let mut v = vec![0.0_f32; self.dim];
            for (i, b) in text.as_bytes().iter().enumerate() {
                let slot = i % self.dim;
                v[slot] += (*b as f32) / 255.0;
            }
            out.push(v);
        }
        Ok(out)
    }
}

#[cfg(feature = "embedding-http")]
/// Blocking HTTP embedding provider plugin.
///
/// Request payload:
/// `{ "texts": [...], "model": "optional" }`
///
/// Response payload:
/// - `{ "embeddings": [[...], ...] }`, or
/// - `[[...], ...]`
#[derive(Debug, Clone)]
pub struct HttpEmbeddingProvider {
    endpoint: String,
    model: Option<String>,
    bearer_token: Option<String>,
    client: reqwest::blocking::Client,
}

#[cfg(feature = "embedding-http")]
impl HttpEmbeddingProvider {
    /// Creates a new HTTP provider.
    pub fn new(
        endpoint: impl Into<String>,
        model: Option<String>,
        bearer_token: Option<String>,
        timeout_secs: u64,
    ) -> Result<Self> {
        if timeout_secs == 0 {
            return Err(FfqError::InvalidConfig(
                "http embedding timeout must be > 0 seconds".to_string(),
            ));
        }
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
            .map_err(|e| FfqError::Execution(format!("http client build failed: {e}")))?;
        Ok(Self {
            endpoint: endpoint.into(),
            model,
            bearer_token,
            client,
        })
    }
}

#[cfg(feature = "embedding-http")]
impl EmbeddingProvider for HttpEmbeddingProvider {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        #[derive(serde::Serialize)]
        struct Req<'a> {
            texts: &'a [String],
            #[serde(skip_serializing_if = "Option::is_none")]
            model: Option<&'a str>,
        }

        #[derive(serde::Deserialize)]
        struct WrappedResp {
            embeddings: Vec<Vec<f32>>,
        }

        let body = Req {
            texts,
            model: self.model.as_deref(),
        };
        let mut req = self.client.post(&self.endpoint).json(&body);
        if let Some(token) = &self.bearer_token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .map_err(|e| FfqError::Execution(format!("embedding http request failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(FfqError::Execution(format!(
                "embedding http request failed: status {}",
                resp.status()
            )));
        }
        let raw: serde_json::Value = resp
            .json()
            .map_err(|e| FfqError::Execution(format!("invalid embedding response JSON: {e}")))?;

        let vectors = if let Ok(wrapped) = serde_json::from_value::<WrappedResp>(raw.clone()) {
            wrapped.embeddings
        } else {
            serde_json::from_value::<Vec<Vec<f32>>>(raw).map_err(|e| {
                FfqError::Execution(format!(
                    "embedding response must be embeddings object or array: {e}"
                ))
            })?
        };
        validate_embedding_result(texts.len(), &vectors)?;
        Ok(vectors)
    }
}

#[cfg(any(test, feature = "embedding-http"))]
fn validate_embedding_result(input_count: usize, vectors: &[Vec<f32>]) -> Result<()> {
    if vectors.len() != input_count {
        return Err(FfqError::Execution(format!(
            "embedding provider returned {} vectors for {} inputs",
            vectors.len(),
            input_count
        )));
    }
    if vectors.is_empty() {
        return Ok(());
    }
    let dim = vectors[0].len();
    if dim == 0 {
        return Err(FfqError::Execution(
            "embedding provider returned zero-dimension vectors".to_string(),
        ));
    }
    for (i, v) in vectors.iter().enumerate() {
        if v.len() != dim {
            return Err(FfqError::Execution(format!(
                "embedding dimension mismatch at index {i}: expected {dim}, got {}",
                v.len()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{EmbeddingProvider, SampleEmbeddingProvider, validate_embedding_result};

    #[test]
    fn sample_provider_embeds_with_fixed_dim() {
        let provider = SampleEmbeddingProvider::new(4).expect("provider");
        let texts = vec!["hello".to_string(), "world".to_string()];
        let out = provider.embed(&texts).expect("embed");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].len(), 4);
        assert_eq!(out[1].len(), 4);
    }

    #[test]
    fn function_provider_plug_in_works() {
        let provider = |texts: &[String]| -> ffq_common::Result<Vec<Vec<f32>>> {
            Ok(texts.iter().map(|_| vec![1.0, 2.0]).collect())
        };
        let texts = vec!["a".to_string(), "b".to_string()];
        let out = provider.embed(&texts).expect("embed");
        assert_eq!(out, vec![vec![1.0, 2.0], vec![1.0, 2.0]]);
    }

    #[test]
    fn validate_embedding_result_checks_count_and_dim() {
        let err = validate_embedding_result(2, &[vec![1.0]]).expect_err("count mismatch");
        assert!(err.to_string().contains("returned 1 vectors for 2 inputs"));

        let err =
            validate_embedding_result(2, &[vec![1.0, 2.0], vec![1.0]]).expect_err("dim mismatch");
        assert!(err.to_string().contains("dimension mismatch"));
    }
}
