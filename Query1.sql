WITH developer_stats AS (
    SELECT
        developer,
        name,
        positive_ratings,
        negative_ratings,
        ROUND(
            (positive_ratings::NUMERIC /
            NULLIF(positive_ratings + negative_ratings, 0) * 100), 2
        ) AS sentiment_pct,
        RANK() OVER (
            PARTITION BY developer
            ORDER BY
                positive_ratings::NUMERIC /
                NULLIF(positive_ratings + negative_ratings, 0) DESC
        ) AS rank_in_developer
    FROM steam_games
    WHERE positive_ratings + negative_ratings > 100
),
top_developers AS (
    SELECT developer
    FROM developer_stats
    GROUP BY developer
    HAVING AVG(sentiment_pct) > 80 AND COUNT(*) >= 3
)
SELECT
    ds.developer,
    ds.name,
    ds.sentiment_pct,
    ds.rank_in_developer
FROM developer_stats ds
JOIN top_developers td ON ds.developer = td.developer
WHERE ds.rank_in_developer <= 3
ORDER BY ds.developer, ds.rank_in_developer;