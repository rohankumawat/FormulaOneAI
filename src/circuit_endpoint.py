# import libraries
import os
import requests
import json
import pandas as pd
import xml.etree.ElementTree as ET

class CircuitDataExtractor:
    def __init__(self, base_url):
        self.base_url = base_url

    def fetch_data(self, endpoint):
        """Fetch data from a specific endpoint."""
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    def parse_data(self, response, format='json'):
        """Parse the response data and return a structured format."""
        if format == 'json':
            return response.json()
        elif format == 'xml':
            root = ET.fromstring(response.content)
            data = []
            for child in root:
                record = {}
                for item in child:
                    record[item.tag] = item.text
                data.append(record)
            return data
    
    def save_data_to_csv(self, data, file_name):
        """Save the data to a CSV file."""
        df = pd.DataFrame(data)
        df.to_csv(file_name, index=False)
        print(f"Data saved to {file_name}")

# Example Usage:
base_url = "http://ergast.com/api/f1/circuits.json"
extractor = CircuitDataExtractor(base_url)
response = extractor.fetch_data("circuits")
data = extractor.parse_data(response)
extractor.save_data_to_csv(data, 'circuits.csv')
