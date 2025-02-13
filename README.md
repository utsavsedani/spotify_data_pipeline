# Spotify Data Pipeline

## Overview
This project implements an **ETL (Extract, Transform, Load) pipeline** for processing and analyzing **Spotify Dataset 2023** using **Python, Kaggle API, Pandas, SQLite, and Matplotlib**.

## Features
- **Extracts data from Kaggle** and downloads the dataset.
- **Cleans and transforms the data** by handling missing values, renaming columns, and standardizing text.
- **Filters popular non-explicit tracks** and adds a `radio_mix` classification.
- **Loads the cleaned data into an SQLite database**.
- **Generates visualizations** for top labels and most popular tracks.

## Technologies Used
- **Python** – Core programming language for the ETL pipeline.
- **Kaggle API** – Fetching the dataset.
- **Pandas** – Data cleaning and transformation.
- **SQLite** – Database for storing structured data.
- **Matplotlib** – Data visualization.

## Pipeline Workflow
1. **Download Data** – Fetches raw Spotify dataset from **Kaggle API**.
2. **Load Data** – Reads CSV files into **Pandas DataFrames**.
3. **Clean Data** – Handles missing values, renames columns, and formats text.
4. **Transform Data** – Filters popular tracks and classifies short songs as `radio_mix`.
5. **Load to Database** – Stores transformed data in **SQLite**.
6. **Visualize Data** – Generates **top labels** and **most popular tracks** reports.

## Setup Instructions
### 1️⃣ Install Dependencies
```bash
pip install pandas kaggle sqlite3 matplotlib
```

### 2️⃣ Set Up Kaggle API
- Get **Kaggle API Token** from [Kaggle Account](https://www.kaggle.com/account).
- Place `kaggle.json` in `~/.kaggle/` (Linux/Mac) or `%USERPROFILE%\.kaggle\` (Windows).

### 3️⃣ Run the Pipeline
```bash
python pipeline.py
```


---
**Author:** Utsav Sedani  
**License:** MIT  
🚀 **Contributions Welcome!**


