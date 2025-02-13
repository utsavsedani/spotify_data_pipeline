#!/usr/bin/env python
# coding: utf-8

import unittest
import pandas as pd
import sqlite3
from pipeline import clean_data, transform_data, load_to_db


class TestSpotifyPipeline(unittest.TestCase):

    def setUp(self):
        """
        Set up sample data for testing.
        """
        self.sample_tracks = pd.DataFrame(
            {
                "track_id": [1, 2, 3],
                "track_name": [" Song One ", "SONG two", "song THREE"],
                "release_date": ["2023-06-01", "invalid_date", "2022-07-15"],
                "label": [" Label1 ", "label2", "LABEL3"],
                "duration_sec": ["180", "250", None],
                "track_popularity": [55, 30, 80],
                "explicit": [False, True, False],
            }
        )

        self.sample_albums = pd.DataFrame(
            {
                "track_id": [1, 2, 3],
                "track_name": [" Song One ", "SONG two", "song THREE"],
                "release_date": ["2023-06-01", "invalid_date", "2022-07-15"],
                "label": [" Label1 ", "label2", "LABEL3"],
                "duration_sec": ["180", "250", "300"],
            }
        )

    def test_clean_data(self):
        """
        Test the clean_data function for proper formatting and data type conversion.
        """
        cleaned_data = clean_data(self.sample_tracks, self.sample_albums)

        # Ensure release_date is converted to datetime
        self.assertTrue(
            pd.api.types.is_datetime64_any_dtype(cleaned_data["release_date"])
        )

        # Ensure duration_sec is converted to Float64
        self.assertTrue(pd.api.types.is_float_dtype(cleaned_data["duration_sec"]))

        # Ensure track_popularity is converted to Int64
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned_data["track_popularity"]))

        # Ensure explicit column is boolean
        self.assertTrue(pd.api.types.is_bool_dtype(cleaned_data["explicit"]))

        # Ensure text fields are properly stripped and formatted
        self.assertEqual(cleaned_data["track_name"].iloc[0], "Song One")
        self.assertEqual(cleaned_data["label"].iloc[0], "Label1")

    def test_transform_data(self):
        """
        Test the transform_data function for filtering logic.
        """
        cleaned_data = clean_data(self.sample_tracks, self.sample_albums)
        transformed_data = transform_data(cleaned_data)

        # Ensure only non-explicit tracks with popularity > 50 are retained
        self.assertEqual(len(transformed_data), 2)
        self.assertTrue((transformed_data["track_popularity"] > 50).all())
        self.assertFalse(transformed_data["explicit"].any())

    def test_load_to_db(self):
        """
        Test the load_to_db function for successful data storage in SQLite.
        """
        conn = sqlite3.connect(":memory:")  # Use an in-memory database for testing
        transformed_data = transform_data(
            clean_data(self.sample_tracks, self.sample_albums)
        )
        load_to_db(transformed_data)

        df = pd.read_sql("SELECT * FROM spotify_tracks", conn)
        self.assertEqual(len(df), len(transformed_data))
        conn.close()


if __name__ == "__main__":
    unittest.main()
