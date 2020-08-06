import asyncio, asyncpg
import json

from DataCollection.GraphQL import GraphQL
from DataCollection.Parser import Parser
from DataCollection.DateBreakdown import DateBreakdown
from datetime import datetime, timedelta

# Setup variables
config = json.load(open("Data/Config.json"))
query = open("src/Queries/Collection.graphql").read()
variables = lambda history, config : {"after": history.get("after"), "first": config["first"]}

parser = Parser()

async def get_data():
    # Set up database
    async with asyncpg.create_pool(config["database"]) as pool:
        async with pool.acquire() as connection:
            # await connection.execute("DROP TABLE IF EXISTS Periods, History, Repositories")

            await connection.execute("CREATE TABLE IF NOT EXISTS Periods (StartDate TIMESTAMP, EndDate TIMESTAMP, NumRepos BIGINT NOT NULL, PRIMARY KEY (StartDate, EndDate))")
            await connection.execute("CREATE TABLE IF NOT EXISTS History (StartDate TIMESTAMP, EndDate TIMESTAMP, NextPage BOOLEAN NOT NULL DEFAULT TRUE, After TEXT DEFAULT NULL, PRIMARY KEY (StartDate, EndDate), FOREIGN KEY (StartDate, EndDate) REFERENCES Periods (StartDate, EndDate))")
            await connection.execute("CREATE TABLE IF NOT EXISTS Repositories (ID TEXT PRIMARY KEY, Name TEXT NOT NULL, Description TEXT, Topics TEXT, License TEXT, README TEXT, PrimaryLanguage TEXT, CreatedDate TIMESTAMP, LastPushedDate TIMESTAMP, Fork BOOLEAN NOT NULL DEFAULT FALSE, Archived BOOLEAN NOT NULL DEFAULT FALSE, Locked BOOLEAN NOT NULL DEFAULT FALSE, ForkCount BIGINT CHECK (ForkCount >= 0), Commits BIGINT CHECK (Commits >= 0), Issues BIGINT CHECK (Issues >= 0), PullRequests BIGINT CHECK (PullRequests >= 0), Users BIGINT CHECK (Users >= 0), Watchs BIGINT CHECK (Watchs >= 0), Stars BIGINT CHECK (Stars >= 0))")

            # Code to reset history logs from periods
            # await connection.execute("INSERT INTO History(StartDate, EndDate) SELECT StartDate, EndDate FROM Periods")

        breakdown = DateBreakdown(config, pool)

        # Finds the needed date divisions to use
        # The whole opperation must run at once (without crashing)
        # format_date = lambda date : datetime.strptime(str(date), "%Y")
        # await breakdown.safe_expand_periods([(format_date(date), format_date(date) + timedelta(days=365)) for date in range(2016, 2020)])

        # Data collection
        await breakdown.collect_between(query, variables, parser)

asyncio.run(get_data())
