# Merid_Aid_Analysis
A data engineering and analytics pipeline that extracts IPEDS university data from Access databases to rank U.S. colleges based on merit aid generosity and affordability for middle-class families.

## College Merit Aid Analysis Pipeline
This project provides an automated way to identify high-value colleges by analyzing the IPEDS (Integrated Postsecondary Education Data System) dataset. It focuses on finding institutions that offer significant institutional grants (merit aid) relative to their sticker price.

How it Works:
The project consists of a two-stage pipeline:

Data Extraction (CSV_Conversion.py): * Connects to the raw Microsoft Access Database (.accdb).

Converts complex relational tables into flat CSV files for processing.

Data Analysis & Ranking (IPEDS_Merit_Aid_Analysis.py):

Cleans and merges data across financial, admissions, and graduation tables.

Calculates the Merit Generosity Index (MGI).

Generates a ranked "Top 20" list of colleges based on a composite score of quality and cost.

Produces interactive visualizations (Dumbbell and Parallel Coordinates charts) using Plotly.

Tech Stack:
Language: Python

Libraries: Pandas, NumPy, Plotly, PyODBC

Data Source: National Center for Education Statistics (NCES) IPEDS Database
