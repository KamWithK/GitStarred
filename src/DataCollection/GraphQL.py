import requests
import json
import re

import pandas as pd

from DataCollection.ConfigManager import ConfigManager
from hashlib import sha256

class GraphQL():
    def __init__(self, token: str):
        self.config_manager = ConfigManager()

        self.headers = {'Authorization': f"token {token}"}

    # Makes a single query
    # View "Data/Temp.json" for raw data
    def basic_query(self, query: str, variables=None):
        json_query = {"query": query, "variables": variables}

        # Continuously retry request until successful
        while True:
            try:
                raw_data = requests.post(url="https://api.github.com/graphql", json=json_query, headers=self.headers).text
                parsed_data = json.loads(raw_data)
                json.dump(parsed_data, open("Data/TempData.json", "w"), indent=2)
                if not "errors" in parsed_data: break
                else: open("Data/TempQuery.graphql", "w").write(query)
            except Exception as identifier:
                print(identifier)

        try:
            return parsed_data["data"]
        except TypeError as error:
            print(json.loads(raw_data))
            print(error)

    # Makes a single query keeping state
    # Needs to have the `after` and `next_page` parameters in the expected location (within `search`, `pageInfo`)
    def try_meta_query(self, query: str, variables={}):
        data = self.basic_query(query, variables)["search"]

        return data["edges"], {"after": data["pageInfo"]["endCursor"], "next_page": data["pageInfo"]["hasNextPage"]}

    # Runs a single query composed of a number of subsections
    def batch_query(self, outer_query: str, inner_queries: dict, variables: dict={}):
        inner_groups = [inner_queries[i:i + 300] for i in range(0, len(inner_queries), 300)]
        outputs = {}

        for query_group in inner_groups:
            inner_query = "\n".join(["{", *query_group, "}"])

            query = re.sub("{.*}", inner_query, outer_query)
            outputs.update(self.basic_query(query, variables))
        
        return outputs

    # Generator for handling pagination
    def get_stream(self, query: str, variables: dict={}):
        new_history = {"next_page": True}
        query_variables = variables

        while new_history["next_page"] == True:
            query_variables["after"] = new_history.get("after")
            output, metadata = self.try_meta_query(query, query_variables)
            new_history = self.config_manager.update(metadata, new_history)

            yield output, new_history
