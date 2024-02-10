# import libraries
import os
import requests
import json
import pandas as pd
import xml.etree.ElementTree as ET

class RaceScheduleDataExtractor:
    def __init__(self, base_url):
        self.base_url = base_url
    
    def fetch_data(self, endpoint, format=None):
        offset = 0
        total = None
        all_data = pd.DataFrame() # Initialize all_data as an empty DataFrame

        while total is None or offset < total:
            """Fetch data from a specific endpoint."""
            url = f"{self.base_url}/{endpoint}?limit=30&offset={offset}"
            print(f"Fetching data from: {url}")

            headers = {'Accept': 'application/json'}
            # Make a GET request to the API
            response = requests.get(url, headers=headers)

            # Check the response status code
            if response.status_code == 200:
                parsed_data = self.parse_data(response, format)
                # all_data.extend(parsed_data)
                # Append parsed_data to all_data
                all_data = pd.concat([all_data, parsed_data], ignore_index=True)

                if format == 'json':
                    mr_data = response.json().get('MRData', {})
                    limit = int(mr_data.get('limit', 0))
                    offset += limit
                    total = int(mr_data.get('total', 0))
                elif format == 'xml':
                    # Parse the XML response
                    root = ET.fromstring(response.content)
                    # Directly access the 'limit', 'offset', and 'total' attributes from the root
                    limit = int(root.attrib.get('limit', 30))
                    offset = int(root.attrib.get('offset', 0))
                    total = int(root.attrib.get('total', 0))
                    parsed_data = self.parse_data(response, format)
                    # Update the offset for the next iteration
                    offset += limit
            
            else:
                # Print response for debugging
                print(f"Failed to fetch data: {response.status_code}")
                print(f"Response: {response.text}")
                break
        
        return all_data
    
    def parse_data(self, response, format):
        """Parse the data based on the format specified."""
        if format == 'json':
            race_schedule_data = response.json()
            return pd.json_normalize(race_schedule_data['MRData']['RaceTable']['Races'])
        elif format == 'xml':
            pass
        else:
            raise ValueError(f"Invalid format: {format}")
    
    def save_data_to_csv(self, data, filename):
        """Save the data to a CSV file."""
        if not filename.endswith('.csv'):
            filename = f"{filename}.csv"
        data.to_csv(filename, index=False)
        print(f"Data saved to: {filename}")
    
    def fetch_all_years_data(self, start_year=1950, end_year=2023):
        all_years_data = pd.DataFrame()
        for year in range(start_year, end_year+1):
            print(f"Fetching data for year: {year}")
            data = self.fetch_data(f"{year}.json", format='json')
            all_years_data = pd.concat([all_years_data, data], ignore_index=True)
        return all_years_data

# Define the base URL for the Ergast API
base_url = "https://ergast.com/api/f1"
# Initialize the RaceScheduleDataExtractor
race_schedule_extractor = RaceScheduleDataExtractor(base_url=base_url)
# Fetch data
all_year_race_schedule_data = race_schedule_extractor.fetch_all_years_data()
# Save the data to a CSV file
race_schedule_extractor.save_data_to_csv(data=all_year_race_schedule_data, filename='../data/all_race_schedule_data.csv')