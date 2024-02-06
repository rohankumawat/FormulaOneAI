# import libraries
import os
import requests
import json
import pandas as pd
import xml.etree.ElementTree as ET

class CircuitDataExtractor:
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

            ########---DEBUG STATEMENTS---########
            # print(f"Status Code: {response.status_code}")
            # print(f"Headers: {response.headers}")
            # print(f"Text: {response.text}")

            # Check the response status code
            if response.status_code == 200:
                parsed_data = self.parse_data(response, format)
                # all_data.extend(parsed_data)
                # Append parsed_data to all_data
                all_data = pd.concat([all_data, parsed_data], ignore_index=True)

                # Debugging print to inspect the structure of parsed_data
                # print(f"Parsed Data: {parsed_data[:2]}")  # Print first 2 records to inspect
                
                if format == 'json': # to change the offset
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
                    limit = int(root.attrib.get('limit', 30))  # Default to 30 if not present
                    offset = int(root.attrib.get('offset', 0))  # Default to 0 if not present
                    total = int(root.attrib.get('total', 0))    # Default to 0 if not present
                    parsed_data = self.parse_data(response, format)
                    # Update the offset for the next iteration
                    offset += limit
            else:
                # Print response for debugging
                print(f"Failed to fetch data: {response.status_code}")
                print(f"Response: {response.text}")
                break  # or handle according to your specific needs
            
        return all_data

    def parse_data(self, response, format=None):
        """Parse the response data and return a structured format."""
        if format == 'json':
            circuit_data = response.json()
            return pd.json_normalize(circuit_data["MRData"]["CircuitTable"]["Circuits"])
        elif format == 'xml':
            root = ET.fromstring(response.content)
            ns = {'mrd': 'http://ergast.com/mrd/1.5'}  # Define the namespace
            circuits = root.findall('.//mrd:Circuit', ns)
            data = []
            for circuit in circuits:
                circuit_id = circuit.attrib['circuitId']
                circuit_name = circuit.find('mrd:CircuitName', ns).text
                location = circuit.find('mrd:Location', ns)
                latitude = location.attrib['lat']
                longitude = location.attrib['long']
                locality = location.find('mrd:Locality', ns).text
                country = location.find('mrd:Country', ns).text
                record = {
                    'circuitId': circuit_id,
                    'circuitName': circuit_name,
                    'Location.lat': latitude,
                    'Location.long': longitude,
                    'Location.locality': locality,
                    'Location.country': country
                }
                data.append(record)
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def save_data_to_csv(self, data, file_name):
        """Save the data to a CSV file."""
        df = pd.DataFrame(data)
        df.to_csv(file_name, index=False)
        print(f"Data saved to {file_name}")
    
    def fetch_all_years_data(self, start_year=1950, end_year=2023):
        all_years_data = pd.DataFrame()

        for year in range(start_year, end_year + 1):
            endpoint = f"{year}/circuits.json"  # Assuming you want JSON format
            yearly_data = self.fetch_data(endpoint, format='json')
            yearly_data['Year'] = year  # Add a column for the year

            all_years_data = pd.concat([all_years_data, yearly_data], ignore_index=True)
        
        return all_years_data

# Example Usage:
base_url = "http://ergast.com/api/f1"
extractor = CircuitDataExtractor(base_url=base_url)
# data = extractor.fetch_data("circuits", format='xml') # "circuits.json" for json
# extractor.save_data_to_csv(data, '../data/circuits.csv')

all_circuits_data = extractor.fetch_all_years_data()
extractor.save_data_to_csv(all_circuits_data, '../data/all_circuits_1950_2023.csv')