import requests
import csv
import os

# Hub configuration dictionary
PRODUCERS_HUB = {
    "Trans Nzoia": {"lat": 1.0507, "lon": 34.9570},
    "Uasin Gishu": {"lat": 0.5527, "lon": 35.3027},
    "Nakuru": {"lat": -0.3071, "lon": 36.0722}
}

def fetch_weather_data(start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    filename = "producer_weather_extract.csv"
    
    print(f"Initiating pipeline for dates: {start_date} to {end_date}\n" + "-"*40)
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        
        writer.writerow([
            'county', 'date', 'temp_max_c', 'temp_min_c', 'precipitation_mm', 
            'wind_speed_max_kmh', 'solar_radiation_mj_m2', 'evapotranspiration_mm'
        ])
        
        for county, coords in PRODUCERS_HUB.items():
            print(f"Fetching data for {county} (Lat: {coords['lat']}, Lon: {coords['lon']})...")
            
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "start_date": start_date,
                "end_date": end_date,
                "daily": [
                    "temperature_2m_max", 
                    "temperature_2m_min", 
                    "precipitation_sum",
                    "wind_speed_10m_max",
                    "shortwave_radiation_sum",
                    "et0_fao_evapotranspiration"
                ],
                "timezone": "Africa/Nairobi"
            }
            
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            raw_data = response.json()
            daily_data = raw_data["daily"]
            num_days = len(daily_data["time"])
            
            for i in range(num_days):
                writer.writerow([
                    county,  # Our Join Key!
                    daily_data["time"][i],
                    daily_data["temperature_2m_max"][i],
                    daily_data["temperature_2m_min"][i],
                    daily_data["precipitation_sum"][i],
                    daily_data["wind_speed_10m_max"][i],
                    daily_data["shortwave_radiation_sum"][i],
                    daily_data["et0_fao_evapotranspiration"][i]
                ])
            
            print(f"Success! Appended {num_days} rows for {county}.\n")
            
    print(f"Pipeline complete. All data successfully saved to {filename}")
    
    
if __name__ == "__main__":
    start_date_str = os.environ.get('START_DATE')
    end_date_str = os.environ.get('END_DATE')
    
    if not start_date_str or not end_date_str:
        raise ValueError("START_DATE and END_DATE environment variables must be set.")
    
    fetch_weather_data(start_date_str, end_date_str)