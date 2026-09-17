WITH price_tiered AS (
    SELECT
        name,
        price,
        average_playtime,
        positive_ratings,
        owners_lower,
        CASE
            WHEN price = 0       THEN 'Free'
            WHEN price <= 5      THEN 'Budget ($0-$5)'
            WHEN price <= 15     THEN 'Mid ($5-$15)'
            WHEN price <= 30     THEN 'Standard ($15-$30)'
            ELSE                      'Premium ($30+)'
        END AS price_tier
    FROM steam_games
    WHERE average_playtime > 0 AND price >= 0
),
tier_summary AS (
    SELECT
        price_tier,
        COUNT(*)                                 AS game_count,
        ROUND(AVG(price)::NUMERIC, 2)            AS avg_price,
        ROUND(AVG(average_playtime)::NUMERIC, 0) AS avg_playtime_mins,
        ROUND(AVG(positive_ratings)::NUMERIC, 0) AS avg_positive_ratings,
        SUM(owners_lower)                        AS total_owners
    FROM price_tiered
    GROUP BY price_tier
)
SELECT
    price_tier,
    game_count,
    avg_price,
    avg_playtime_mins,
    ROUND((avg_playtime_mins / 60.0)::NUMERIC, 1) AS avg_playtime_hours,
    avg_positive_ratings,
    total_owners,
    RANK() OVER (ORDER BY avg_playtime_mins DESC) AS playtime_rank,
    RANK() OVER (ORDER BY avg_positive_ratings DESC) AS rating_rank
FROM tier_summary
ORDER BY avg_playtime_mins DESC;