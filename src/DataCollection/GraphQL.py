import asyncio, aiohttp
import json
import re

import pandas as pd

from aiohttp import ClientSession
from tenacity import retry
from tenacity import wait_full_jitter
from itertools import cycle
from collections import ChainMap
from hashlib import sha256

class GraphQL():
    def __init__(self, tokens: list, proxy: str):
        self.tokens = cycle(tokens)
        self.proxy = proxy
        self.next_header = lambda : {"Authorization": f"token {next(self.tokens)}"}
        self.session = None

    # Makes a single query
    # View "Data/Temp.json" for raw data
    @retry(wait=wait_full_jitter())
    async def basic_query(self, query: str, variables=None, semaphore=None):
        json_query = {"query": query, "variables": variables}

        # Added in limits (haven't tested yet)
        if self.session == None: self.session = ClientSession(timeout=aiohttp.ClientTimeout(connect=120))

        if semaphore != None:
            async with semaphore:
                async with self.session.post("https://api.github.com/graphql", headers=self.next_header(), json=json_query, proxy=self.proxy) as response:
                    parsed_data = await response.json()
        else:
            async with self.session.post("https://api.github.com/graphql", headers=self.next_header(), json=json_query, proxy=self.proxy) as response:
                parsed_data = await response.json()

        json.dump(parsed_data, open("Data/TempData.json", "w"), indent=2)

        # Trigger retrying the query when an error is found
        if "errors" in parsed_data: raise ValueError

        try:
            return parsed_data["data"]
        except TypeError as error:
            print(error)

    # Makes a single query keeping state
    # Needs to have the `after` and `next_page` parameters in the expected location (within `search`, `pageInfo`)
    async def try_meta_query(self, query: str, variables={}, semaphore=None):
        data = await self.basic_query(query, variables, semaphore)
        data = data["search"]

        return data["edges"], {"after": data["pageInfo"]["endCursor"], "next_page": data["pageInfo"]["hasNextPage"]}

    # Runs a single query composed of a number of subsections
    async def batch_query(self, outer_query: str, inner_queries: dict, variables: dict={}):
        inner_groups = [inner_queries[i:i + 300] for i in range(0, len(inner_queries), 300)]
        outputs = {}

        async def single_batch_query(query_group):
            inner_query = "\n".join(["{", *query_group, "}"])

            query = re.sub("{.*}", inner_query, outer_query)
            return await self.basic_query(query, variables)

        outputs = await asyncio.gather(*[single_batch_query(query_group) for query_group in inner_groups])
        return dict(ChainMap(*outputs))

    # Generator for handling pagination
    async def get_stream(self, query: str, variables: dict={}):
        new_history = {"next_page": True}
        query_variables = variables

        while new_history["next_page"] == True:
            query_variables["after"] = new_history.get("after")
            output, metadata = await self.try_meta_query(query, query_variables)
            new_history.update(metadata)

            yield output, new_history
