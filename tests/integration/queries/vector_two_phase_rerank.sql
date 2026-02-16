SELECT id, title
FROM docs
WHERE lang = 'en'
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT 1
