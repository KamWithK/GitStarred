import pandas as pd
import requests
import json

from DataCollection.GraphQL import GraphQL
from collections import ChainMap
from hashlib import sha256

class DateBreakdown():
    def __init__(self, config_path, history_path):
        self.config = json.load(open(config_path))

        self.config_path = config_path
        self.history_path = history_path

        self.query = GraphQL(self.config["token"])

        # Subquery to use when batching
        self.search = open("src/Queries/InnerDateTester.graphql").read()

    # Checks whether each period is small enough for full data collection
    def valid_periods(self, periods):
        # Format strings in specific ways for GraphQL
        # IDs must start with an alphabetic character, so h is put at the start
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        argument = lambda period : f"created:{date_format(period.start_time)}..{date_format(period.end_time)}"
        id_hash = lambda period : f"h{sha256(date_format(period).encode()).hexdigest()}"

        # Execute all checks in one batch query
        searches = [self.search.format(id=id_hash(period), argument=argument(period)) for period in periods]
        less_than_1000 = lambda section : section["repositoryCount"] < 1000

        return map(less_than_1000, self.query.batch_query("{}", searches).values())

    # Divide one large period into two half as small
    def half_period(self, original_period):
        freq = original_period.freq / 2
        first_period = original_period.asfreq(freq, how="start")
        second_period = pd.Period(first_period.end_time, freq)

        return first_period, second_period

    # Breath first search for valid date periods
    # Something passing in an empty list (not `safe_expand_periods`)
    def expand_periods(self, periods):
        periods_converged = []
        trial_periods = []

        # Sort periods into two lists
        for period, is_valid in zip(periods, self.valid_periods(periods)):
            if is_valid == True:
                periods_converged.append(period)
            else:
                trial_periods.extend(self.half_period(period))

        if trial_periods != []:
            return [*periods_converged, *self.expand_periods(trial_periods)]
        else:
            pd.DataFrame(periods_converged).to_pickle("../Data/Periods.pickle")
            return periods_converged

    # Ensures no future dates (after today) are passed into `expand_periods`
    def safe_expand_periods(self, periods):
        safe_periods = [period for period in periods if period.start_time < pd.to_datetime("now")]

        for period in safe_periods:
            if period.end_time > pd.to_datetime("now"):
                safe_periods.remove(period)
                safe_periods.append(pd.Period(period.start_time, pd.to_datetime("now") - period.start_time))

        return self.expand_periods(safe_periods)

    def collect_period(self, period, query: str, variables={}):
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        start, end = date_format(period.start_time), date_format(period.end_time)
        variables["conditions"] = f"is:public sort:created created:{start}..{end}"

        return self.query.get_stream(query, variables)

    # Collects data between a list of periods
    def collect_between(self, periods, query: str, select_variables, history: dict):
        date_format = lambda date : date.strftime("%Y-%m-%dT%H:%M:%d")
        id_hash = lambda period : f"h{sha256(date_format(period).encode()).hexdigest()}"

        for period in periods:
            current_id = id_hash(period)

            # Collect data on new and partially existing periods
            history[current_id] = history.get(current_id, {})
            history[current_id]["next_page"] = history[current_id].get("next_page", True)

            if history[current_id]["next_page"] == True:
                variables = select_variables(history[current_id], self.config)

                for output, new_history in self.collect_period(period, query, variables):
                    history.update({current_id: new_history})

                    yield output, history
