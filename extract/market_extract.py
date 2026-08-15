import requests
import csv
import io

# Hubs configuration
CONSUMER_HUBS = ["Nairobi", "Mombasa", "Turkana"]

# static direct-download URL for the WFP Kenya Food Prices dataset
WFP_CSV_URL = "https://data.humdata.org/dataset/e0d3fba6-f9a2-45d7-b949-140c455197ff/resource/517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/wfp_food_prices_ken.csv"

def fetch_market_data():
    filename = "consumer_market_extract.csv"
    print("Initiating WFP Market Data extraction...\n" + "-"*40)
    
    # downloads the csv data into memory 
    print("Downloading dataset from HDX...")
    response = requests.get(WFP_CSV_URL)
    response.raise_for_status()
    
    # convert the downloaded raw text into a format  the CSV  reader can understand 
    raw_csv_data = io.StringIO(response.text)
    reader = csv.reader(raw_csv_data)
    
    # read the headers in csv
    headers = next(reader)
    
    # extract the index positions of the columns we need for this project
    try:
        date_idx = headers.index("date")
        county_idx = headers.index("admin2")
        market_idx = headers.index("market")
        commodity_idx = headers.index("commodity")
        unit_idx = headers.index("unit")
        price_idx = headers.index("price")
    except ValueError as e:
        raise ValueError(f"Schema changed! Could not find expected column: {e}")
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write our clean, standardized headers
        writer.writerow(['date', 'county', 'market', 'commodity', 'unit', 'price_kes'])
        
        row_count = 0
        
        # Loop through the downloaded data row by row
        for row in reader:
            # HDX datasets often have a second header row with #tags (e.g., #date, #adm2). We skip it.
            if row[0] == "#date":
                continue
                
            county_name = row[county_idx]
            
            # The Filter: Only save the row if it belongs to our 3 Consumer Hubs
            if county_name in CONSUMER_HUBS:
                writer.writerow([
                    row[date_idx],
                    county_name,      
                    row[market_idx],
                    row[commodity_idx],
                    row[unit_idx],
                    row[price_idx]
                ])
                row_count += 1
                
    print(f"Success! Filtered and saved {row_count} records for {CONSUMER_HUBS}.")
    print(f"Data saved to {filename}")
    
if __name__ == "__main__":
    fetch_market_data()