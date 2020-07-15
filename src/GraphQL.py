import requests
import json
import pandas as pd

from pandas.io.json import json_normalize

class GraphQL():
    def __init__(self, query, config_path):
        self.config_path, self.config = config_path, json.load(open(config_path))
        self.headers = {'Authorization': f"token {self.config['token']}"}
        self.query = query
        self.query["variables"] = {}

        self.update_params(self.config["variables"], True)

    # Syncs changing parameters
    def update_params(self, args: dict, for_query=False):
        for variable, value in args.items():
            if for_query == False:
                self.config[variable] = value
            else:
                self.query["variables"][variable] = value
                self.config["variables"][variable] = value
        
        json.dump(self.config, open(self.config_path, "w"), indent=2)

    # Makes a single query
    # View "Data/Temp.json" for raw data
    def try_query(self):
        # Continuously retry request until successful
        while True:
            raw_data = requests.post(url="https://api.github.com/graphql", json=self.query, headers=self.headers).text
            parsed_data = json.loads(raw_data)
            json.dump(parsed_data, open("Data/Temp.json", "w"), indent=2)
            if not "errors" in parsed_data: break

        try:
            self.update_params({"after": parsed_data["data"]["search"]["pageInfo"]["endCursor"]}, True)
            self.update_params({"next_page": parsed_data["data"]["search"]["pageInfo"]["hasNextPage"]})

            return parsed_data["data"]["search"]["edges"]
        except TypeError as error:
            print(json.loads(raw_data))
            print(error)

    # Generator for handling pagination
    def get_stream(self):
        while self.config["next_page"] == True:
            yield self.try_query()
