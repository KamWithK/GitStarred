import os
import asyncio
import pickle
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
    periods = asyncio.run(breakdown.safe_expand_periods(pd.period_range("2006", pd.to_datetime("now"), freq="525600 T")))

# Data collection
# Setup variables
query = open("src/Queries/Collection.graphql").read()
history = json.load(open("Data/History.json")) if os.path.exists("Data/History.json") else {}
parser = Parser(pd.read_pickle("Data/Data.pickle")) if os.path.exists("Data/Data.pickle") else Parser()

# Loop through data as it is collected
variables = lambda history, config : {"after": history.get("after"), "first": config["first"]}
history = asyncio.run(breakdown.collect_between(periods[0], query, variables, history, parser))
