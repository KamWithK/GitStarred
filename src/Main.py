import os
import pickle
import requests
import json

import pandas as pd

from GraphQL import GraphQL
from Parser import Parser
from DateBreakdown import DateBreakdown

# Finds the needed date divisions to use
if os.path.exists("Data/Periods.pickle"):
    periods = pd.read_pickle("Data/Periods.pickle")
else:
    breakdown = DateBreakdown("Data/Config.json")
    periods = breakdown.safe_expand_periods(pd.period_range("2006", pd.to_datetime("now"), freq="525600 T"))
    
    for period in periods:
        print(period)

# Actual data collection
query = {"query": open("src/Queries/Collection.graphql").read()}
query = GraphQL(query, "Data/Config.json")

parser = Parser(pd.read_pickle("Data/Data.pickle")) if os.path.exists("Data/Data.pickle") else Parser()

# Code to reset values to their original state
# query.update_params({"first": 100, "after": None, "conditions": "is:public sort:created"}, True)
# query.update_params({"next_page": True})

results = query.get_stream()

for json_data in results:
    parser.append(json_data)

    print(f"{len(parser.data)} now downloaded")
    parser.data.to_pickle("Data/Data.pickle")
