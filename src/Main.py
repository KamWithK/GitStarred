import requests
import json
import pandas as pd

from GraphQL import GraphQL
from Parser import Parser

query = {"query": open("src/Query.graphql").read()}

config = json.load(open("Data/Config.json"))
query = GraphQL(query, "Data/Config.json")
results = query.get_stream()
parser = None

# Code to reset values to their original state
# query.update_params({"first": 100, "after": None, "conditions": "is:public sort:created"}, True)
# query.update_params({"next_page": True})

for json_data in results:
    if parser == None: parser = Parser(json_data)
    else: parser.append(json_data)

    print(f"{len(parser.data)} now downloaded")
    parser.data.to_pickle("Data/Data.pickle")
