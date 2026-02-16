SELECT id, title
FROM docs
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT 2
