# import libraries
import os
import requests
import json
import pandas as pd
import xml.etree.ElementTree as ET

class DriversDataExtractor:
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