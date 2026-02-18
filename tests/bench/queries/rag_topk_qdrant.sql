SELECT id, score, payload
FROM docs
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT 10
