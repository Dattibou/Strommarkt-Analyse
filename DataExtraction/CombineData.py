import pandas as pd
import os


def merge_csvs_on_time(output_file="merged.csv"):
    """
    Merge CSVs from 'smard_data' and 'weather_data' on 'time_berlin'.
    Only retain rows where both datasets have data, dropping any with NaN.

    Parameters
    ----------
    output_file : str
        Path to save the merged CSV
    """

    # --- Set Folders where CSVs are located ---
    smard_folder = "smard_data"
    os.makedirs(smard_folder, exist_ok=True)
    weather_folder = "weather_data"
    os.makedirs(weather_folder, exist_ok=True)
    # --- Read CSVs ---
    smard_file = os.path.join(smard_folder, "combined_smard_data.csv")
    weather_data = os.path.join(weather_folder, "weather_avg_data.csv")

    if not os.path.exists(smard_file):
        raise FileNotFoundError(f"No CSV files found in {smard_folder}")
    if not os.path.exists(weather_data):
        raise FileNotFoundError(f"No CSV files found in {weather_folder}")

    # Assume one file per folder (A1.csv, B1.csv)
    df_smard = pd.read_csv(smard_file, parse_dates=["time_berlin"])
    df_weather = pd.read_csv(weather_data, parse_dates=["time_berlin"])

    # --- Merge on time ---
    merged = pd.merge(
        df_smard, df_weather,
        on="time_berlin",
        how="outer",  # keep all timestamps
        sort=True  # sort by time
    )

    # Drop any row with NaN in any column
    merged_clean = merged.dropna()

    # Save
    merged_clean.to_csv(output_file, index=False)
    print(f"Merged {smard_file} and {weather_data} to {output_file}")


if __name__ == "__main__":
    merge_csvs_on_time()