import os
import asyncio
import pickle
import json

import pandas as pd

from databases import Database
from DataCollection.GraphQL import GraphQL
from DataCollection.Parser import Parser
from DataCollection.DateBreakdown import DateBreakdown

# Setup variables and database
query = open("src/Queries/Collection.graphql").read()
variables = lambda history, config : {"after": history.get("after"), "first": config["first"]}
parser = Parser(pd.read_pickle("Data/Data.pickle")) if os.path.exists("Data/Data.pickle") else Parser()

database = Database("sqlite://../Data/History.db")
create_history = "CREATE TABLE IF NOT EXISTS History (ID TEXT PRIMARY KEY, NextPage INTEGER NOT NULL DEFAULT 1 CHECK (NextPage in (0, 1)), After TEXT)"

asyncio.run(database.connect())
asyncio.run(database.execute(create_history))

breakdown = DateBreakdown("Data/Config.json", database)

# Finds the needed date divisions to use
if os.path.exists("Data/Periods.pickle"):
    periods = pd.read_pickle("Data/Periods.pickle")
else:
    periods = asyncio.run(breakdown.safe_expand_periods(pd.period_range("2006", pd.to_datetime("now"), freq="525600 T")))

# Data collection
asyncio.run(breakdown.collect_between(periods[0], query, variables, parser))

# Disconnect database
asyncio.run(database.disconnect())
