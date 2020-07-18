import requests
import json
import re

import pandas as pd

from pandas.io.json import json_normalize
from hashlib import sha256

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
    def basic_query(self):
        # Continuously retry request until successful
        while True:
            try:
                raw_data = requests.post(url="https://api.github.com/graphql", json=self.query, headers=self.headers).text
                parsed_data = json.loads(raw_data)
                json.dump(parsed_data, open("Data/TempData.json", "w"), indent=2)
                if not "errors" in parsed_data: break
                else: open("Data/TempQuery.graphql", "w").write(self.query["query"])
            except Exception as identifier:
                print(identifier)

        try:
            return parsed_data["data"]
        except TypeError as error:
            print(json.loads(raw_data))
            print(error)

    # Makes a single query keeping state
    # Needs to have the `after` and `next_page` parameters in the expected location (within `search`, `pageInfo`)
    def try_meta_query(self):
        data = self.basic_query()["search"]

        self.update_params({"after": data["pageInfo"]["endCursor"]}, True)
        self.update_params({"next_page": data["pageInfo"]["hasNextPage"]})

        return data["edges"]

    # Runs a single query composed of a number of subsections
    def batch_query(self, outer_query, inner_queries: dict):
        inner_groups = [inner_queries[i:i + 300] for i in range(0, len(inner_queries), 300)]
        outputs = {}

        for query_group in inner_groups:
            inner_query = "\n".join(["{", *query_group, "}"])

            self.query["query"] = re.sub("{.*}", inner_query, outer_query)
            outputs.update(self.basic_query())
        
        if len(outputs) != len(inner_queries): print("FAIL")
        
        return outputs

    # Generator for handling pagination
    def get_stream(self):
        while self.config["next_page"] == True:
            yield self.try_meta_query()

    def collect_between(self, periods):
        continue_from_here = True if self.config["current_period"] == None else False
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        id_hash = lambda period : f"h{sha256(date_format(period).encode()).hexdigest()}"

        for period in periods:
            current_id = id_hash(period)

            if self.config["current_period"] == current_id:
                continue_from_here = True
            if continue_from_here==True:
                self.update_params({"conditions": f"is:public sort:created created:{date_format(period.start_time)}..{date_format(period.end_time)}"}, True)
                self.update_params({"current_period": current_id})

                yield self.get_stream()

            self.update_params({"after": None}, True)
            self.update_params({"next_page": True})
