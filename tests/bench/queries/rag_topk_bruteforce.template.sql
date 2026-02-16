SELECT id, title, lang
FROM {{table}}
{{where_clause}}
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT {{k}}
