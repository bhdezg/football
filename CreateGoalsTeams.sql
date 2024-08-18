BEGIN
    CREATE TABLE IF NOT EXISTS goals_team AS
    SELECT 
        t1.*, 
        SUM(t1.goals) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS goals_last_5,
        SUM(t1.goals_c) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS goalsc_last_5,
        SUM(CASE WHEN t1.side = 1 THEN CASE WHEN t1.goals > 0 THEN 1 ELSE 0 END ELSE NULL END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS home_goals_last_5,
        SUM(CASE WHEN t1.side = 2 THEN CASE WHEN t1.goals > 0 THEN 1 ELSE 0 END ELSE NULL END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS away_goals_last_5,
        MIN(t1.fixture_date) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS later_fixture_date,
        SUM(t1.winner) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS wins_last_15,
        SUM(CASE WHEN t1.goals_c > t1.goals THEN 1 ELSE 0 END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS lost_last_15,
        MIN(t1.fixture_date) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS later_fixture_date_15,
        CASE WHEN MAX(t1.fixture_date) OVER (PARTITION BY t1.team_id) = t1.fixture_date THEN 1 ELSE 0 END AS latest_fixture,
        SUM(CASE WHEN t1.goals_halftime = 0 THEN 0 ELSE 1 END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS goals_firsthalf_10,
        SUM(CASE WHEN t1.goals > t1.goals_halftime THEN 1 ELSE 0 END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS goals_secondhalf_10,
        SUM(CASE WHEN t1.goals + t1.goals_c >= 1 THEN 1 ELSE 0 END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS one_goals_last_15,
        SUM(CASE WHEN t1.goals + t1.goals_c >= 2 THEN 1 ELSE 0 END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS two_goals_last_15,
        SUM(CASE WHEN t1.goals + t1.goals_c >= 3 THEN 1 ELSE 0 END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS three_goals_last_15,
        SUM(CASE WHEN t1.goals + t1.goals_c >= 4 THEN 1 ELSE 0 END) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS four_goals_last_15,
        COUNT(*) OVER (PARTITION BY t1.team_id ORDER BY t1.fixture_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS games_last_15
    FROM (
        SELECT
            fixture_id AS fixture_id,
            fixture_date AS fixture_date,
            teams_home_id AS team_id,
            score_fulltime_home AS goals,
            score_fulltime_away AS goals_c,
            score_halftime_home AS goals_halftime,
            score_halftime_away AS goals_halftime_c,
            CASE WHEN teams_home_winner = 'TRUE' THEN 1 ELSE 0 END AS winner,
            1 AS 'side'
        FROM
            fixtures
        WHERE
            fixture_status_long = 'Match Finished'
        UNION ALL
        SELECT
            fixture_id AS fixture_id,
            fixture_date AS fixture_date,
            teams_away_id AS team_id,
            score_fulltime_away AS goals,
            score_fulltime_home AS goals_c,
            score_halftime_away AS goals_halftime,
            score_halftime_home AS goals_halftime_c,
            CASE WHEN teams_away_winner = 'TRUE' THEN 1 ELSE 0 END AS winner,
            2 AS 'side'
        FROM
            fixtures
        WHERE
            fixture_status_long = 'Match Finished'
    ) t1;
END
