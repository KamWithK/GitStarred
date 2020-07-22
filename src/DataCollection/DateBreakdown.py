import asyncio
import requests
import json

import pandas as pd

from DataCollection.GraphQL import GraphQL
from databases import Database
from collections import ChainMap
from hashlib import sha256

class DateBreakdown():
    def __init__(self, config_path, database):
        self.config = json.load(open(config_path))
        self.database = database

        self.config_path = config_path

        self.query = GraphQL(self.config["tokens"])

        # Subquery to use when batching
        self.search = open("src/Queries/InnerDateTester.graphql").read()

    # Checks whether each period is small enough for full data collection
    async def valid_periods(self, periods):
        # Format strings in specific ways for GraphQL
        # IDs must start with an alphabetic character, so h is put at the start
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        argument = lambda period : f"created:{date_format(period.start_time)}..{date_format(period.end_time)}"
        id_hash = lambda period : f"h{sha256(date_format(period).encode()).hexdigest()}"

        # Execute all checks in one batch query
        searches = [self.search.format(id=id_hash(period), argument=argument(period)) for period in periods]
        less_than_1000 = lambda section : section["repositoryCount"] < 1000

        return map(less_than_1000, (await self.query.batch_query("{}", searches)).values())

    # Divide one large period into two half as small
    def half_period(self, original_period):
        freq = original_period.freq / 2
        first_period = original_period.asfreq(freq, how="start")
        second_period = pd.Period(first_period.end_time, freq)

        return first_period, second_period

    # Breath first search for valid date periods
    # Something passing in an empty list (not `safe_expand_periods`)
    async def expand_periods(self, periods):
        periods_converged = []
        trial_periods = []

        # Sort periods into two lists
        for period, is_valid in zip(periods, await self.valid_periods(periods)):
            if is_valid == True:
                periods_converged.append(period)
            else:
                trial_periods.extend(self.half_period(period))

        if trial_periods != []:
            new_periods = await self.expand_periods(trial_periods)
            return [*periods_converged, *new_periods]
        else:
            pd.DataFrame(periods_converged).to_pickle("Data/Periods.pickle")
            return pd.DataFrame(periods_converged)

    # Ensures no future dates (after today) are passed into `expand_periods`
    async def safe_expand_periods(self, periods):
        safe_periods = [period for period in periods if period.start_time < pd.to_datetime("now")]

        for period in safe_periods:
            if period.end_time > pd.to_datetime("now"):
                safe_periods.remove(period)
                safe_periods.append(pd.Period(period.start_time, pd.to_datetime("now") - period.start_time))

        return await self.expand_periods(safe_periods)

    async def collect_period(self, period, query: str, select_variables, parser):
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        current_id = f"h{sha256(date_format(period).encode()).hexdigest()}"
        start, end = date_format(period.start_time), date_format(period.end_time)

        # Collect data on new and partially existing periods
        next_page = await self.database.fetch_one("SELECT DISTINCT NextPage FROM History WHERE ID=:ID", {"ID": current_id})
        after = await self.database.fetch_one("SELECT DISTINCT After FROM History WHERE ID=:ID", {"ID": current_id})

        if next_page == True or next_page == None:
            variables = select_variables({"next_page": next_page, "after": after}, self.config)
            variables["conditions"] = f"is:public sort:created created:{start}..{end}"

            async for output, new_history in self.query.get_stream(query, variables):
                replace_query = "REPLACE INTO History(ID, NextPage, After) VALUES (:ID, :NextPage, :After)"
                values = {"ID": current_id, "NextPage": new_history["next_page"], "After": new_history["after"]}
                await self.database.execute(replace_query, values)
                await asyncio.get_running_loop().run_in_executor(None, lambda : parser.append(output))

    # Collects data between a list of periods
    async def collect_between(self, periods, query: str, select_variables, parser):
        await asyncio.gather(*[self.collect_period(period, query, select_variables, parser) for period in periods])
        
        if self.query.session != None: await self.query.session.close()
