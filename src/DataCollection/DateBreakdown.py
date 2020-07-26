import asyncio
import requests
import json
import sqlite3

import pandas as pd

from DataCollection.GraphQL import GraphQL
from fork_futures import ForkPoolExecutor
from collections import ChainMap
from hashlib import sha256

class DateBreakdown():
    def __init__(self, config_path, database_path):
        self.config = json.load(open(config_path))

        self.config_path = config_path
        self.database_path = database_path

        self.query = GraphQL(self.config["tokens"], self.config["proxy"])

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

    # Evaluates whether to continue with a period
    def period_producer(self, period, period_id, condition, after=None, next_page=None):
        # Find previous history from database where not fed into function
        if after == None and next_page == None:
            database = sqlite3.connect(self.database_path)
            next_page = database.cursor().execute("SELECT DISTINCT NextPage FROM History WHERE ID=?", (period_id,)).fetchone()
            after = database.cursor().execute("SELECT DISTINCT After FROM History WHERE ID=?", (period_id,)).fetchone()
            database.close()

            # Ensures data is collected for new entries
            next_page = True if next_page == None else next_page[0]
            after = None if after == None else after[0]

        if next_page == True:
            return (period, period_id, next_page, after, condition)

    # Saves and processes data
    def process_data(self, period_id, output, after, next_page, parser):
        # Update history database
        database = sqlite3.connect(self.database_path)
        replace_query = "REPLACE INTO History(ID, NextPage, After) VALUES (:ID, :NextPage, :After)"
        database.execute(replace_query, {"ID": period_id, "NextPage": next_page, "After": after})
        database.commit()
        database.close()

        # Process and save new data
        parser.append(output)

    # Collect and process a single page of data for one period
    async def processor(self, period, period_id, next_page, after, condition, query: str, select_variables, parser, semaphore, pool):
        variables = select_variables({"next_page": next_page, "after": after}, self.config)
        variables["conditions"] = condition
        output, metadata = await self.query.try_meta_query(query, variables, semaphore)
        after, next_page = metadata.get("after"), metadata.get("next_page", True)

        # Run lengthy parsing and saving on a seperate process
        pool.submit(self.process_data, period_id, output, after, next_page, parser)

        return self.period_producer(period, period_id, condition, after, next_page)

    # Collects data for a list of periods
    async def collect_between(self, periods, query: str, select_variables, parser):
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        period_id = lambda period : f"h{sha256(date_format(period).encode()).hexdigest()}"
        condition = lambda period : f"is:public sort:created created:{date_format(period.start_time)}..{date_format(period.end_time)}"
        period_producer = lambda period : self.period_producer(period, period_id(period), condition(period))

        # Ensure steady download rate
        semaphore = asyncio.Semaphore(10000)
        pool = ForkPoolExecutor()

        query_periods = await asyncio.gather(*[asyncio.get_event_loop().run_in_executor(None, period_producer, period) for period in periods])
        print("Finished preprocessing periods")

        # Continue to re-query each period until everything is collected
        while len(query_periods) > 0:
            query_periods = await asyncio.gather(*[self.processor(*query_period, query, select_variables, parser, semaphore, pool) for query_period in query_periods if query_period != None])
        
        pool.shutdown()
        if self.query.session != None: await self.query.session.close()
