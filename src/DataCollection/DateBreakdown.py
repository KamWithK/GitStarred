import asyncio, asyncpg

from DataCollection.GraphQL import GraphQL
from datetime import datetime
from collections import ChainMap
from hashlib import sha256

class DateBreakdown():
    def __init__(self, config: dict, pool: asyncpg.pool.Pool):
        self.config = config
        self.pool = pool

        self.query = GraphQL(self.config["tokens"], self.config["proxy"])

        # Subquery to use when batching
        self.search = open("src/Queries/InnerDateTester.graphql").read()

    # Checks whether each period is small enough for full data collection
    async def valid_periods(self, periods: list):
        # Format strings in specific ways for GraphQL
        # IDs must start with an alphabetic character, so h is put at the start
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        argument = lambda start, end: f"created:{date_format(start)}..{date_format(end)}"
        id_hash = lambda time_string : f"h{sha256(time_string.encode()).hexdigest()}"
        period_id = lambda start, end : id_hash(date_format(start) + "  " + date_format(end))

        # Execute all checks in one batch query
        # Return as a list of tuples (with their period, number of repos and whether valid)
        searches = [self.search.format(id=period_id(*period), argument=argument(*period)) for period in periods]
        get_info = lambda entry : (entry[0], entry[1]["repositoryCount"], entry[1]["repositoryCount"] < 1000)

        return map(get_info, zip(periods, (await self.query.batch_query("{}", searches)).values()))

    # Divide one large period into two half as small
    def half_period(self, start, end):
        return (start, start + (end - start) / 2), (start + (end - start) / 2, end)

    # Breath first search for valid date periods (under 1000 results each)
    # By continuously halving the search space
    async def expand_periods(self, periods: list):
        while True:
            trial_periods = []

            # Handle one period at a time
            for period, num_repos, is_valid in await self.valid_periods(periods):
                if is_valid == True and num_repos > 0:
                    async with self.pool.acquire() as connection:
                        await connection.execute("INSERT INTO Periods(StartDate, EndDate, NumRepos) VALUES ($1, $2, $3)", *period, num_repos)
                else:
                    trial_periods.extend(self.half_period(*period))

            if trial_periods == []: break
            else: periods = trial_periods

        # Prepopulate history
        async with self.pool.acquire() as connection:
            await connection.execute("INSERT INTO History(StartDate, EndDate) SELECT StartDate, EndDate FROM Periods")

    # Ensures no future dates (after today) are passed into `expand_periods`
    async def safe_expand_periods(self, periods: list):
        safe_periods = periods

        for period in periods:
            if period[0] > datetime.now():
                safe_periods.remove(period)
            elif period[1] > datetime.now():
                safe_periods.remove(period)
                safe_periods.append((period[0], datetime.now()))

        await self.expand_periods(safe_periods)

    # Collect and process a single page of data for one period
    async def processor(self, start, end, next_page, after, condition, query: str, select_variables, semaphore, parser):
        variables = select_variables({"next_page": next_page, "after": after}, self.config)
        variables["conditions"] = condition
        output, metadata = await self.query.try_meta_query(query, variables, semaphore)
        next_page, after = metadata.get("next_page", True), metadata.get("after")

        # Log progress and process/save data to database before continuing
        # Either progress and data is saved or neither (through a single transaction)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                replace_query = "UPDATE History SET NextPage=$3, After=$4 WHERE StartDate=$1 AND EndDate=$2"
                await parser.commit_data(output, connection)
                await connection.execute(replace_query, start, end, next_page, after)

        if next_page == True: return (start, end, next_page, after, condition)

    # Collects data for a list of periods
    async def collect_between(self, query: str, select_variables, parser):
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        condition = lambda period_data : f"is:public sort:created created:{date_format(period_data[0])}..{date_format(period_data[1])}"

        # Ensure steady download rate
        # Using a semaphore to limit concurrent query attempts
        semaphore = asyncio.Semaphore(15000)

        # Import periods which haven't yet been collected
        async with self.pool.acquire() as connection:
            history = [(*period_data, condition(period_data)) for period_data in await connection.fetch("SELECT * FROM History WHERE NextPage=True")]

        # Continue to re-query each period until all data is collected
        print("Started loading data...")
        while len(history) > 0:
            history = await asyncio.gather(*[self.processor(*period_data, query, select_variables, semaphore, parser) for period_data in history])

        # Close everything and release all resources after completion
        if self.query.session != None: await self.query.session.close()
        print("\nFINISHED")
