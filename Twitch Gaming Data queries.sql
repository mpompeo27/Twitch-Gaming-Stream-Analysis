--Task 1: Select first 20 rows from the stream and chat tables.
SELECT *
FROM stream
LIMIT 20;
--Columns: time | device_id | login | channel | country | player | game | stream_format | subscriber

SELECT *
FROM chat
LIMIT 20;
--Columns: time | device_id | login | channel | country | player | game

--Task 2: What are the unique games in the stream table?
SELECT DISTINCT game
FROM stream;

--Task 3: What are the unique channels in the stream table?
SELECT DISTINCT channel
FROM stream;

--Task 4: What are the most popular games in the stream table? Create a list of games and their number of viewers using GROUP BY.
SELECT game, 
  COUNT(*) AS num_viewers
FROM stream
GROUP BY 1
ORDER BY 2 DESC;

--Task 5: Create a list of countries and their number of League of Legends viewers using WHERE and GROUP BY.
SELECT country, 
  COUNT(*)
FROM stream
WHERE game = 'League of Legends'
GROUP BY 1
ORDER BY 2 DESC;

--Task 6: The player column contains the source the user is using to view the stream (site, iphone, android, etc). Create a list of players and their number of streamers.
SELECT player, 
  COUNT(*)
FROM stream
WHERE player IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;

--Task 7: Using CASE, create a new column named genre for each of the games. Group the games into their genres: Multiplayer Online Battle Arena (MOBA), First Person Shooter (FPS), Survival, and Other. 
SELECT game,
  CASE
    WHEN game IN ('League of Legends', 'Dota 2', 'Heroes of the Storm') THEN 'MOBA'
    WHEN (game = 'Counter-Strike: Global Offensive') THEN 'FPS'
    WHEN game IN ('DayZ', 'ARK: Survival Evolved') THEN 'Survival'
    ELSE 'OTHER'
  END AS genre,
  COUNT(*)
FROM stream
GROUP BY 1
ORDER BY 3 DESC;

--Task 8: Run the provided query to see the time column of the stream table. 
SELECT time
FROM stream
LIMIT 10;
--Note format YYYY-MM-DD HH:MM:SS

--Task 9: Test out STRFTIME using the query provided.
SELECT time,
   strftime('%S', time)
FROM stream
GROUP BY 1
LIMIT 20;
--This will return the time in one column and only the seconds from the time in the second column.

--Task 10: Write a query that retuns the hours of the time column and the view count for each hour, limited to only users in my country.
SELECT STRFTIME('%H', time) AS hour_of_day,
  COUNT(*) AS num_viewers
FROM stream
WHERE country = 'US'
GROUP BY 1;

--Task 11: The stream table and the chat table share a column: device_id. Let’s join the two tables on that column.
SELECT *
FROM stream s
JOIN chat c
  ON s.device_id = c.device_id;

--Task 12: See what else you can dig up within the data.
--Which channels are the most watched?
SELECT channel,
  COUNT(DISTINCT login) AS num_viewers
FROM stream
GROUP BY 1
ORDER BY 2 DESC;

--Which channels have the most viewers and most subscribers participating in the chat?
SELECT c.channel, 
  COUNT(DISTINCT s.login) AS subs_in_chat
FROM chat c
JOIN stream s
  ON c.device_id = s.device_id
WHERE s.subscriber = 'True'
GROUP BY 1
ORDER BY 2 DESC;

--How many total chat messages did each channel have? Which chat was most active by percent of viewers participating?
--Result limited to channels with at least once chat message.
SELECT s.channel, 
  COUNT(c.time) AS num_chat_messages,
  COUNT(DISTINCT s.login) AS num_unique_viewers,
  COUNT(DISTINCT c.login) AS num_viewers_in_chat,
  ROUND(100.0 * COUNT(DISTINCT c.login) / COUNT(DISTINCT s.login), 2) AS pct_viewers_in_chat
FROM stream s
LEFT JOIN chat c
  ON s.device_id = c.device_id
GROUP BY 1
  HAVING COUNT(c.time) > 0
ORDER BY 5 DESC;

--How many total subscribers watched each channel? What percentage were they of all viewers?
--Create temporary table counting the number of unique subscriber viewers for each channel
WITH subscribers AS (
  SELECT channel, 
    COUNT(DISTINCT login) AS num_sub_viewers
  FROM stream
  WHERE subscriber = 'True'
  GROUP BY 1
),
--Temporary table counting all unique viewers for each channel
viewers AS (
  SELECT channel,
    COUNT(DISTINCT login) AS num_viewers 
  FROM stream
  GROUP BY 1
)
--LEFT JOIN subscribers to viewers and get the percentage of all viewers that were subscribers for each game
SELECT v.channel,
  --Use COALESCE() to replace NULL values with zero for games that had no subscriber viewers 
  COALESCE(s.num_sub_viewers, 0) AS num_sub_viewers,
  v.num_viewers, 
  ROUND(100.0 * COALESCE(s.num_sub_viewers, 0) / v.num_viewers, 2) AS pct_sub_viewers
FROM viewers v
--Use LEFT JOIN to preserve the full list of games even if they had no subscriber viewers
LEFT JOIN subscribers s
  ON v.channel = s.channel
--Group by channel
GROUP BY 1
--Order by number of sub viewers, then by total viewers, both descending
ORDER BY 2 DESC, 3 DESC;

--How many total subscribers watched each game? What percentage were they of all viewers?
--Create temporary table counting the number of unique subscriber viewers for each game
WITH subscribers AS (
  SELECT game, 
    COUNT(DISTINCT login) AS num_sub_viewers
  FROM stream
  WHERE subscriber = 'True'
  GROUP BY 1
),
--Temporary table counting all unique viewers for each game
viewers AS (
  SELECT game,
    COUNT(DISTINCT login) AS num_viewers 
  FROM stream
  GROUP BY 1
)
--LEFT JOIN subscribers to viewers and get the percentage of all viewers that were subscribers for each game
SELECT v.game,
  --Use COALESCE() to replace NULL values with zero for games that had no subscriber viewers 
  COALESCE(s.num_sub_viewers, 0) AS num_sub_viewers,
  v.num_viewers, 
  ROUND(100.0 * COALESCE(s.num_sub_viewers, 0) / v.num_viewers, 2) AS pct_sub_viewers
FROM viewers v
--Use LEFT JOIN to preserve the full list of games even if they had no subscriber viewers
LEFT JOIN subscribers s
  ON v.game = s.game
--Group by game
GROUP BY 1
--Order by number of sub viewers, then by total viewers, both descending
ORDER BY 2 DESC, 3 DESC;

--How does the total number of viewers change hour-by-hour throughout the day? Use STRFTIME to extract the hour from the time, COUNT the number of viewers for each hour, and use window functions to show the change from hour to hour, the running total number of viewers, a running average number of viewers per hour, and the overall average number of viewers per hour
SELECT hour_of_day,
  num_viewers,
  --Change in hourly number of viewers
  num_viewers - LAG(num_viewers, 1) OVER (
    ORDER BY hour_of_day
  ) AS change_num_viewers,
  --Running total for number of viewers
  SUM(num_viewers) OVER (
    ORDER BY hour_of_day
  ) AS running_total_viewers,
  --Running average for number of viewers per hour rounded to 2 decimal places
  ROUND(AVG(num_viewers) OVER (
    ORDER BY hour_of_day
  ), 2) AS running_avg_viewers,
  --Overall average number of viewers per hour rounded to 2 decimal places. Use window function with no PARTITION or ORDER BY to repeat on every row
  ROUND(AVG(num_viewers) OVER (), 2) AS overall_avg_viewers
FROM (
  --Subquery to extract the hour from the time and count the number of viewers during that hour
  SELECT STRFTIME('%H', time) AS hour_of_day,
    COUNT(DISTINCT login) AS num_viewers
  FROM stream
  GROUP BY 1
) subq;

--repeat the same query showing the change in viewers throughout the day but broken down by game and make sure to show all hours for all games even if they had no viewers
--Create a list of all games
WITH games AS (
  SELECT DISTINCT game
  FROM stream
  WHERE game IS NOT NULL
),
--Create a list of all hours
hours AS (
  SELECT DISTINCT STRFTIME('%H', time) AS hour_of_day
  FROM stream
),
--CROSS JOIN to get all unique game and hour combos
game_hour_combos AS (
  SELECT *
  FROM games
  CROSS JOIN hours
),
--Aggregate the viewer counts of each game per hour
viewers_agg AS (
  SELECT game, 
    STRFTIME('%H', time) AS hour_of_day,
    COUNT(DISTINCT login) AS num_viewers
  FROM stream
  GROUP BY 1, 2 
)
SELECT ghc.game,
  ghc.hour_of_day,
  COALESCE(v.num_viewers, 0) AS num_viewers,
  --Change in hourly number of viewers
  COALESCE(v.num_viewers, 0) - LAG(COALESCE(v.num_viewers, 0), 1) OVER (
    PARTITION BY ghc.game
    ORDER BY ghc.hour_of_day
  ) AS change_num_viewers,
  --Running total for number of viewers
  SUM(COALESCE(v.num_viewers, 0)) OVER (
    PARTITION BY ghc.game
    ORDER BY ghc.hour_of_day
  ) AS running_total_viewers,
  --Running average for number of viewers per hour rounded to 2 decimal places
  ROUND(AVG(COALESCE(v.num_viewers, 0)) OVER (
    PARTITION BY ghc.game
    ORDER BY ghc.hour_of_day
  ), 2) AS running_avg_viewers,
  --Overall average number of viewers per hour rounded to 2 decimal places. Use window function with no PARTITION or ORDER BY to repeat on every row
  ROUND(AVG(COALESCE(v.num_viewers, 0)) OVER (
    PARTITION BY ghc.game
  ), 2) AS overall_avg_viewers
FROM game_hour_combos ghc
--LEFT JOIN to preserve all game-hour combinations even if no viewers
LEFT JOIN viewers_agg v
  ON ghc.game = v.game
  AND ghc.hour_of_day = v.hour_of_day;

--Which games are the most popular in each country by number of streams? Find the top 3 per country and their average viewership.
SELECT country, 
  rank,   
  game,
  num_streams,
  ROUND(AVG(num_streams) OVER (
    PARTITION BY COUNTRY
  ), 2) AS avg_num_streams
FROM (
  --This subquery structure is required because an aliased column for a window function like rank cannot be used in a HAVING clause after the GROUP BY. Putting the rank of all games per country by number of streams into a subquery then allows us to filter by WHERE rank <= 3 in the external query to return each country's top 3 games only.
  SELECT 
    RANK() OVER(
      PARTITION BY country
      ORDER BY COUNT(TIME) DESC
    ) AS rank,
    country,
    game,
    COUNT(time) AS num_streams
  FROM stream
  WHERE country IS NOT NULL
    AND game IS NOT NULL
  GROUP BY 2,3
)
WHERE rank <=3;

--Find the top country with the most streams for each channel and the % of total streams.
SELECT channel, 
  rank,
  country,
  num_streams,
  ROUND(100.0 * num_streams / total_streams, 2) AS pct_of_total_streams
FROM (
  SELECT 
    RANK() OVER(
      PARTITION BY channel
      ORDER BY COUNT(time) DESC
    ) AS rank,
    channel,
    country,
    COUNT(time) AS num_streams,
    SUM(COUNT(time)) OVER (
      PARTITION BY channel
    ) AS total_streams
  FROM stream
  WHERE country IS NOT NULL
  GROUP BY 2, 3
)
WHERE rank = 1;


