# football

## Introduction
This project automates the management of sports fixture data and provides analytical insights into team performance. The daily.py script handles the data pipeline for extracting and storing fixture data, while the CreateGoalsTeams.sql script analyzes this data to track team performance over recent games.

## Files Included
- daily.py: A Python script that extracts sports fixture data for a specific date, transforms it into a suitable format, and stores it in a database. This script ensures that the fixture data is consistently updated for further analysis.

- CreateGoalsTeams.sql: An SQL script that analyzes the fixture table to track the goals scored and received by a team in the previous 5 games. It also counts the number of fixtures where a team finished with 0 to 4 goals in the last 15 games. This analysis helps in assessing team performance trends.

- execute_sp.py: A Python script that executes the CreateGoalsTeams.sql file. This script is useful for automating the SQL execution process using orchestrator software like Airflow or PowerAutomate.

- odds.py: A Python script used to extract the odds from different bookmakers for the winner of a match and the number of goals in a game. This script enables the analysis of betting trends and probabilities.

## Requirements

1. Python 3.x installed on your system.
2. A compatible SQL database system (e.g., MySQL).
3. An account in https://www.api-football.com/
