# football
This repository contains Python and SQL scripts designed for the automated processing and analysis of sports fixture data. The project includes a Python script for data extraction, transformation, and storage, as well as an SQL script for analyzing team performance metrics.

##Introduction
This project automates the management of sports fixture data and provides analytical insights into team performance. The daily.py script handles the data pipeline for extracting and storing fixture data, while the CreateGoalsTeams.sql script analyzes this data to track team performance over recent games.

##Files Included
- daily.py: A Python script that extracts sports fixture data for a specific date, transforms it into a suitable format, and stores it in a database. This script ensures that the fixture data is consistently updated for further analysis.

- CreateGoalsTeams.sql: An SQL script that analyzes the fixture table to track the goals scored and received by a team in the previous 5 games. It also counts the number of fixtures where a team finished with 0 to 4 goals in the last 15 games. This analysis helps in assessing team performance trends.
