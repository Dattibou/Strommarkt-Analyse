import requests
import pandas as pd
import numpy as np
import os


# --- Step 2: Create grid points (2° steps) ---
def get_grid_points(lat_min, lat_max, lon_min, lon_max):
    lat_points = np.arange(lat_min, lat_max, 2.0)
    lon_points = np.arange(lon_min, lon_max, 2.0)
    grid_points = [(lat, lon) for lat in lat_points for lon in lon_points]
    return grid_points

# --- Step 3: define Function to fetch data from Open-Meteo ---
def fetch_point_data(lat, lon, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,wind_speed_100m,shortwave_radiation",
        "timezone": "Europe/Berlin",
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    return df.set_index("time")

# --- Step 4: Loop over grid points & aggregate ---
def fetch_germany_average(grid_points, start_date, end_date):
    dfs = []
    for lat, lon in grid_points:
        try:
            df = fetch_point_data(lat, lon, start_date, end_date)
            dfs.append(df)
        except Exception as e:
            print(f"Failed at {lat},{lon}: {e}")
    # Combine all dataframes
    combined = pd.concat(dfs, axis=1, keys=[f"{lat},{lon}" for lat, lon in grid_points])
    # Average across grid points for each variable
    avg_df = pd.DataFrame({
        "temperature_2m_°C": combined.xs("temperature_2m", axis=1, level=1).mean(axis=1), # returns series with datetime as index
        "wind_speed_100m_km/h": combined.xs("wind_speed_100m", axis=1, level=1).mean(axis=1),
        "shortwave_radiation_W/m²": combined.xs("shortwave_radiation", axis=1, level=1).mean(axis=1),
    })
    avg_df.index.name = 'time_berlin'
    return avg_df

def run(lat_min, lat_max, lon_min, lon_max, start_date, end_date):
    grid_points = get_grid_points(lat_min, lat_max, lon_min, lon_max)
    df_germany_average = fetch_germany_average(grid_points, start_date, end_date)

    folder_name = "weather_data"
    os.makedirs(folder_name, exist_ok=True)
    file_name = os.path.join(folder_name, "weather_avg_data.csv")
    df_germany_average.to_csv(file_name)
    print(f"CSV saved to {file_name}")


if __name__ == "__main__":
    # --- Step 1: Define Germany bounding box & dates---
    lat_ger_min, lat_ger_max = 47.2, 55.1
    long_ger_min, lon_ger_max = 5.9, 15.0
    start = "2025-09-01"
    end = "2025-09-22"

    run(lat_ger_min,lat_ger_max, long_ger_min, lon_ger_max, start, end)





