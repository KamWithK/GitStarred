import os
import pickle
import requests
import json

import pandas as pd

from DataCollection.GraphQL import GraphQL
from DataCollection.Parser import Parser
from DataCollection.DateBreakdown import DateBreakdown

breakdown = DateBreakdown("Data/Config.json", "Data/History.json")

# Finds the needed date divisions to use
if os.path.exists("Data/Periods.pickle"):
    periods = pd.read_pickle("Data/Periods.pickle")
else:
    periods = breakdown.safe_expand_periods(pd.period_range("2006", pd.to_datetime("now"), freq="525600 T"))

    for period in periods:
        print(period)

# Data collection
# Setup variables
query = open("src/Queries/Collection.graphql").read()
config = json.load(open("Data/Config.json"))
history = json.load(open("Data/History.json")) if os.path.exists("Data/History.json") else {}
parser = Parser(pd.read_pickle("Data/Data.pickle")) if os.path.exists("Data/Data.pickle") else Parser()

# Loop through data as it is collected
variables = lambda history, config : {"after": history.get("after"), "first": config["first"]}
results = breakdown.collect_between(periods[0], query, variables, history)

for output, history in results:
    parser.append(output)
    json.dump(history, open("Data/History.json", "w"), indent=2)

    print(f"{len(parser.data)} now downloaded")
    parser.data.to_pickle("Data/Data.pickle")
