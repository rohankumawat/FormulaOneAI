# import libraries
import os
import requests
import json
import pandas as pd
import xml.etree.ElementTree as ET

class ConstructorsDataExtractor:
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

                # Debugging print to inspect the structure of parsed_data
                # print(f"Parsed Data: {parsed_data[:2]}")  # Print first 2 records to inspect
                
                if format == 'json':
                    mr_data = response.json().get('MRData', {})
                    limit = int(mr_data.get('limit', 0))
                    offset += limit
                    total = int(mr_data.get('total', 0))
                elif format == 'xml':
                    # Debugging: Print the first 500 characters of the XML
                    print(response.content[:500])
                    
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
        """Parse the data based on the format."""
        if format == 'json':
            constructors_data = response.json()
            return pd.json_normalize(constructors_data['MRData']['ConstructorTable']['Constructors'])
        elif format == 'xml':
            return self.parse_xml(response)
        else:
            raise ValueError(f"Invalid format: {format}")
    
    def save_data_to_csv(self, data, filename):
        """Save the data to a CSV file."""
        if not filename.endswith('.csv'):
            filename += '.csv'
        data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    
    def fetch_all_years_data(self, start_year, end_year):
        """Fetch data for all years within the range."""
        all_years_data = pd.DataFrame()

        for year in range(start_year, end_year+1):
            endpoint = f"{year}/constructors.json" # Update the endpoint to include the year
            year_data = self.fetch_data(endpoint=endpoint, format='json')
            year_data['year'] = year # Add a new column to store the year
            all_years_data = pd.concat([all_years_data, year_data], ignore_index=True)
        
        return all_years_data

# Define the base URL
base_url = "http://ergast.com/api/f1"
# Initialize the ConstructorsDataExtractor
constructors_extractor = ConstructorsDataExtractor(base_url=base_url)
# Fetch data from the constructors endpoint
# constructors_data = constructors_extractor.fetch_data(endpoint='constructors.json', format='json')
# Save the data to a CSV file
# constructors_extractor.save_data_to_csv(data=constructors_data, filename='../data/constructors_data.csv')

# Fetch data for all years from 1950 to 2023
all_years_constructors_data = constructors_extractor.fetch_all_years_data(start_year=1950, end_year=2023)
# Save the data to a CSV file
constructors_extractor.save_data_to_csv(data=all_years_constructors_data, filename='../data/all_constructors_1950_2023.csv')