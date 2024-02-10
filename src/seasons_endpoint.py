# import libraries
import os
import requests
import json
import pandas as pd
import xml.etree.ElementTree as ET

class SeasonsDataExtractor:
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
            seasons_data = response.json()
            # Extract the 'SeasonTable' data from the JSON response
            seasons = seasons_data.get('MRData', {}).get('SeasonTable', {}).get('Seasons', [])
            # Create a DataFrame from the JSON data
            df = pd.DataFrame(seasons)
            return df
        elif format == 'xml':
            # Parse the XML response
            root = ET.fromstring(response.content)
            # Extract the 'SeasonTable' data from the XML response
            seasons = root.find('SeasonTable').findall('Season')
            # Create a DataFrame from the XML data
            data = []
            for season in seasons:
                season_data = {
                    'season': season.find('season').text,
                    'url': season.find('url').text
                }
                data.append(season_data)
            df = pd.DataFrame(data)
            return df
        else:
            return pd.DataFrame()
    
    def save_data_to_csv(self, data, filename):
        """Save the data to a CSV file."""
        if not filename.endswith('.csv'):
            filename += '.csv'
        data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

base_url = "http://ergast.com/api/f1"
# Initialize the SeasonsDataExtractor
data_extractor = SeasonsDataExtractor(base_url=base_url)
# Fetch data from the seasons endpoint
seasons_data = data_extractor.fetch_data('seasons.json', format='json')
# Save the data to a CSV file
data_extractor.save_data_to_csv(data=seasons_data, filename='../data/seasons_data.csv')