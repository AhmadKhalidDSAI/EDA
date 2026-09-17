WITH yearly_releases AS (
    SELECT
        release_year,
        COUNT(*) AS total_games,
        ROUND(AVG(price)::NUMERIC, 2) AS avg_price,
        ROUND(AVG(
            positive_ratings::NUMERIC /
            NULLIF(positive_ratings + negative_ratings, 0) * 100
        )::NUMERIC, 2) AS avg_sentiment_pct
    FROM steam_games
    WHERE release_year > 0
    GROUP BY release_year
)
SELECT
    release_year,
    total_games,
    avg_price,
    avg_sentiment_pct,
    LAG(total_games) OVER (ORDER BY release_year) AS prev_year_games,
    total_games - LAG(total_games) OVER (ORDER BY release_year) AS yoy_growth,
    ROUND(
        (total_games - LAG(total_games) OVER (ORDER BY release_year)) * 100.0 /
        NULLIF(LAG(total_games) OVER (ORDER BY release_year), 0), 2
    ) AS growth_pct
FROM yearly_releases
ORDER BY release_year;