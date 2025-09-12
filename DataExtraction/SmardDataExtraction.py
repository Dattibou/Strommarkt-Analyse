from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
import pandas as pd
import os

def get_utc_timestamp_from_date(year: int, month: int, day: int) -> int:

    berlin = ZoneInfo("Europe/Berlin")
    dt_berlin = datetime(year, month, day, 0, 0, 0, tzinfo=berlin)

    # Convert to timestamp (seconds since epoch)
    timestamp_seconds = dt_berlin.timestamp()

    # Convert to milliseconds for Smard API
    timestamp_ms = int(timestamp_seconds * 1000)

    return timestamp_ms



def find_latest_smard_daily_dataset(start_timestamp: int, max_days_back: int = 14):
    """
    Try to find the latest SMARD dataset, starting from start_timestamp,
    checking up to max_days_back days back.
    """
    base_url = "https://www.smard.de/app/chart_data/410/DE/410_DE_hour_{}.json"

    for i in range(max_days_back):
        # Subtract i days from start_timestamp
        timestamp_ms = start_timestamp - i * 24 * 60 * 60 * 1000
        url = base_url.format(timestamp_ms)

        response = requests.get(url)
        if response.status_code == 200:
            return timestamp_ms

    print(f"No daily dataset found in the last {max_days_back} days.")
    return None


def generate_weekly_timestamps(start_timestamp: int) -> list[int]:
    """
    Generate weekly timestamps (7-day intervals) from a given start timestamp
    up to the current week.

    Args:
        start_timestamp (int): Valid SMARD timestamp in milliseconds.

    Returns:
        list[int]: List of weekly timestamps (ms).
    """
    results = []
    one_week_ms = 7 * 24 * 60 * 60 * 1000

    # Normalize "now" to this week's Monday 00:00 UTC
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Floor to Monday of this week
    weekday = now.weekday()  # Monday = 0
    monday_this_week = now.replace(day=now.day - weekday)
    now_ts = int(monday_this_week.timestamp() * 1000)

    ts = start_timestamp
    while ts <= now_ts:
        results.append(ts)
        ts += one_week_ms

    return results


def get_smard_timeseries(filter: int, region: str, resolution: str, timestamp: int):
    """
    Fetch data from the SMARD API.

    Args:
        filter (str): The filter parameter, e.g. '410' consumption
        region (str): The region parameter, e.g. 'DE'
        resolution (str): The resolution, e.g. 'hour', 'quarterhour', 'day'
        timestamp (int): Optional UNIX timestamp (milliseconds).

        check swagger: https://smard.api.bund.dev/

    Returns:
        dict: Parsed JSON response, or None if request fails.
    """

    url = f"https://www.smard.de/app/chart_data/{filter}/{region}/{filter}_{region}_{resolution}_{timestamp}.json"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("series", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def datasets_to_csv(datasets: dict[str, list[list]], csv_filename: str):
    """
    Combine multiple timestamped datasets into a single CSV.

    Args:
        datasets: Dictionary of datasets. Keys are column names (e.g., "price", "demand")
                  Values are lists of [timestamp, value].
        csv_filename: Output CSV file path.
    """
    combined_dict = {}

    # Iterate over each dataset
    for column_name, data in datasets.items():
        for ts, value in data:
            if ts not in combined_dict:
                combined_dict[ts] = {}
            combined_dict[ts][column_name] = value

    # Convert to DataFrame
    df = pd.DataFrame.from_dict(combined_dict, orient='index')
    df.index.name = 'timestamp'
    df.sort_index(inplace=True)

    # Add human-readable datetime column (Berlin time)
    berlin_tz = ZoneInfo("Europe/Berlin")
    df.insert(
        0,
        "datetime_berlin",
        [datetime.fromtimestamp(ts / 1000, tz=berlin_tz).strftime("%Y-%m-%d %H:%M:%S")
         for ts in df.index]
    )

    # Export to CSV
    df.to_csv(csv_filename, float_format='%.2f')
    print(f"CSV saved to {csv_filename}")


def run_pipeline(year: int, month: int, day: int):
    """
    Generate a CSV file containing hourly electricity prices and demand.
    """
    timestamp = get_utc_timestamp_from_date(year=year, month=month, day=day)
    valid_timestamp = find_latest_smard_daily_dataset(timestamp)


    if not valid_timestamp:
        print("No valid dataset found.")
        return

    # Generate all weekly timestamps until this week
    valid_timestamp_list = generate_weekly_timestamps(valid_timestamp)
    # Loop through all weekly timestamps
    for ts in valid_timestamp_list:
        monday = datetime.fromtimestamp(ts / 1000.0)
        prices = get_smard_timeseries(filter=4169, region="DE", resolution="hour", timestamp=ts)
        demands = get_smard_timeseries(filter=410, region="DE", resolution="hour", timestamp=ts)

        folder_name = "smard_data"
        os.makedirs(folder_name, exist_ok=True)
        file_name = os.path.join(folder_name,f"data_{monday.strftime('%Y_%m_%d')}.csv")
        if prices is not None and demands is not None:
            dataset = {
                "price (MWh)": prices,
                "demand (MW)": demands
            }
            datasets_to_csv(dataset, file_name)
        else:
            print(f"Oops, something went wrong for {file_name}")




if __name__ == "__main__":
    run_pipeline(2025, 9, 2)
