SELECT id, title, lang
FROM docs
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT 10
