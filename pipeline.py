#!/usr/bin/env python
# coding: utf-8

"""
Spotify Data Pipeline
This script automates the process of downloading, cleaning, transforming, and visualizing Spotify dataset.
It follows an ETL (Extract, Transform, Load) pipeline approach using Kaggle API, Pandas, SQLite, and Matplotlib.
"""

import os
import pandas as pd
import kaggle
import sqlite3
import matplotlib.pyplot as plt


def download_kaggle_dataset():
    """
    Downloads the Spotify dataset from Kaggle and extracts it to the 'data/' directory.
    """
    try:
        kaggle.api.authenticate()
        dataset_name = "tonygordonjr/spotify-dataset-2023"
        save_path = "data/"
        kaggle.api.dataset_download_files(dataset_name, path=save_path, unzip=True)
        print(f"Dataset downloaded and extracted to {save_path}")
    except Exception as e:
        print(f"Error downloading dataset: {e}")


def load_data():
    """
    Loads the Spotify albums and tracks data from CSV files into Pandas DataFrames.

    Returns:
        albums (DataFrame): Contains album-related data.
        tracks (DataFrame): Contains track-related data.
    """
    try:
        albums = pd.read_csv("data\spotify-albums_data_2023.csv")
        tracks = pd.read_csv("data\spotify_tracks_data_2023.csv")
        return albums, tracks
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"Error loading data: {e}")


def clean_data(tracks, albums):
    """
    Cleans and merges the albums and tracks data.

    Args:
        tracks (DataFrame): Raw tracks dataset.
        albums (DataFrame): Raw albums dataset.

    Returns:
        DataFrame: Cleaned and merged dataset.
    """
    try:
        tracks.rename(columns={"id": "track_id"}, inplace=True)
        albums = albums[
            ["track_id", "track_name", "release_date", "label", "duration_sec"]
        ]
        result = pd.merge(albums, tracks, on="track_id", how="left")

        result["release_date"] = pd.to_datetime(result["release_date"], errors="coerce")
        result["duration_sec"] = pd.to_numeric(
            result["duration_sec"], errors="coerce"
        ).astype("Float64")
        result["track_popularity"] = pd.to_numeric(
            result["track_popularity"], errors="coerce"
        ).astype("Int64")
        result["explicit"] = result["explicit"].astype(bool)
        result["track_name"] = result["track_name"].str.strip().str.title()
        result["label"] = result["label"].str.strip().str.title()

        return result
    except Exception as e:
        print(f"Error cleaning data: {e}")


def transform_data(tracks):
    """
    Transforms data by adding a 'radio_mix' column and filtering popular non-explicit tracks.

    Args:
        tracks (DataFrame): Cleaned dataset.

    Returns:
        DataFrame: Transformed dataset.
    """
    try:
        tracks["radio_mix"] = tracks["duration_sec"] <= 180  # True if song <= 3 mins
        transformed_tracks = tracks[
            (tracks["explicit"] == False) & (tracks["track_popularity"] > 50)
        ]
        return transformed_tracks
    except Exception as e:
        print(f"Error transforming data: {e}")


def load_to_db(tracks):
    """
    Loads the transformed data into an SQLite database.

    Args:
        tracks (DataFrame): Transformed dataset.
    """
    try:
        conn = sqlite3.connect("spotify.db")
        tracks.to_sql("spotify_tracks", conn, if_exists="replace", index=False)
        conn.close()
        print("Data loaded into database successfully.")
    except Exception as e:
        print(f"Error loading data into database: {e}")


def visualize_data():
    """
    Generates and saves visualizations for:
    1. Top 20 labels by track count.
    2. Top 25 popular tracks between 2020 and 2023.
    """
    try:
        conn = sqlite3.connect("spotify.db")
        df_labels = pd.read_sql(
            "SELECT label, COUNT(*) as track_count FROM spotify_tracks GROUP BY label ORDER BY track_count DESC LIMIT 20",
            conn,
        )
        df_tracks = pd.read_sql(
            "SELECT track_name, track_popularity FROM spotify_tracks WHERE release_date BETWEEN '2020-01-01' AND '2023-12-31' ORDER BY track_popularity DESC LIMIT 25",
            conn,
        )
        conn.close()

        plt.figure(figsize=(12, 5))
        plt.barh(df_labels["label"], df_labels["track_count"], color="skyblue")
        plt.xlabel("Number of Tracks")
        plt.ylabel("Label")
        plt.title("Top 20 Labels by Track Count")
        plt.gca().invert_yaxis()
        plt.savefig("top_20_labels_by_track_count.png")
        plt.show()

        plt.figure(figsize=(12, 5))
        plt.barh(
            df_tracks["track_name"], df_tracks["track_popularity"], color="lightcoral"
        )
        plt.xlabel("Track Popularity")
        plt.ylabel("Track Name")
        plt.title("Top 25 Popular Tracks (2020-2023)")
        plt.gca().invert_yaxis()
        plt.savefig("top_25_popular_tracks_between_2020_to_2023.png")
        plt.show()
    except Exception as e:
        print(f"Error in visualization: {e}")


def main():
    """
    Main execution pipeline.
    """
    try:
        download_kaggle_dataset()
        albums, tracks = load_data()
        cleaned_tracks = clean_data(tracks, albums)
        transformed_tracks = transform_data(cleaned_tracks)
        load_to_db(transformed_tracks)
        visualize_data()
    except Exception as e:
        print(f"Pipeline execution error: {e}")


if __name__ == "__main__":
    main()
