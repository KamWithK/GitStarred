import os
import asyncio
import pickle
import json
import sqlite3

import pandas as pd

from DataCollection.GraphQL import GraphQL
from DataCollection.Parser import Parser
from DataCollection.DateBreakdown import DateBreakdown

# Set up database
database = sqlite3.connect("Data/History.db")
database.execute("CREATE TABLE IF NOT EXISTS History (ID TEXT PRIMARY KEY, NextPage INTEGER NOT NULL DEFAULT 1 CHECK (NextPage in (0, 1)), After TEXT)")
database.commit()
database.close()

# Setup variables
query = open("src/Queries/Collection.graphql").read()
variables = lambda history, config : {"after": history.get("after"), "first": config["first"]}

parser = Parser(pd.read_pickle("Data/Data.pickle")) if os.path.exists("Data/Data.pickle") else Parser()
breakdown = DateBreakdown("Data/Config.json", "Data/History.db")

# Finds the needed date divisions to use
if os.path.exists("Data/Periods.pickle"):
    periods = pd.read_pickle("Data/Periods.pickle")
else:
    periods = asyncio.run(breakdown.safe_expand_periods(pd.period_range("2006", pd.to_datetime("now"), freq="525600 T")))

# Data collection
# Loop through data as it is collected
asyncio.run(breakdown.collect_between(periods[0], query, variables, parser))
