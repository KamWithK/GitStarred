import json
import pandas as pd

class Parser():
    data = pd.DataFrame(columns=["Name", "ID", "Description", "ForkCount", "Fork", "Archived", "Locked", "CreatedDate", "LastPushedDate", "PrimaryLanguage", "Users", "Stars", "Watchs", "Issues", "PullRequests", "Topics", "License", "Commits", "README"])

    def __init__(self, data=None):
        if type(data) == pd.DataFrame:
            self.data = self.data.append(data, ignore_index=True)
        elif data != None:
            for repo in data:
                temp = pd.DataFrame(columns=self.data.columns).append(self.remap_json(repo["node"]), ignore_index=True)
                self.data = self.data.append(self.fix_dtypes(temp), ignore_index=True)

    # Avoid TypeErrors due to None types within nested dictionaries
    def safe_parse_list(self, json, keys: list):
        entry = json

        for key in keys:
            if entry == None: break
            entry = entry[key]

        return entry

    # Avoid TypeErrors due to None types within many nested dictionaries
    def safe_parse(self, json, values: dict):
        entry = {}

        for field, keys in values.items():
            entry[field] = self.safe_parse_list(json, keys)

        return entry

    # Parses data on the json "node" level
    def remap_json(self, json):
        values = self.safe_parse(json, {
            "Name": ["name"],
            "ID": ["id"],
            "Description": ["description"],
            "ForkCount": ["forkCount"],
            "Fork": ["isFork"],
            "Archived": ["isArchived"],
            "Locked": ["isLocked"],
            "CreatedDate": ["createdAt"],
            "LastPushedDate": ["pushedAt"],
            "PrimaryLanguage": ["primaryLanguage", "name"],
            "Users": ["assignableUsers", "totalCount"],
            "Stars": ["stargazers", "totalCount"],
            "Watchs": ["watchers", "totalCount"],
            "Issues": ["issues", "totalCount"],
            "PullRequests": ["pullRequests", "totalCount"],
            "Topics": ["repositoryTopics", "edges"],
            "License": ["licenseInfo", "key"],
            "Commits": ["commits", "history", "totalCount"],
            "README": ["readme", "text"]
        })

        values["Topics"] = [self.safe_parse_list(edge, ["node", "topic", "name"]) for edge in self.safe_parse_list(json, ["repositoryTopics", "edges"])]

        return values

    def fix_dtypes(self, data):
        data["ForkCount"] = pd.to_numeric(data["ForkCount"])
        data["Users"] = pd.to_numeric(data["Users"])
        data["Stars"] = pd.to_numeric(data["Stars"])
        data["Watchs"] = pd.to_numeric(data["Watchs"])
        data["Issues"] = pd.to_numeric(data["Issues"])
        data["PullRequests"] = pd.to_numeric(data["PullRequests"])
        data["Commits"] = pd.to_numeric(data["Commits"])

        data["Fork"].fillna(False, inplace=True)
        data["Archived"].fillna(False, inplace=True)
        data["Locked"].fillna(False, inplace=True)

        data["Fork"] = data["Fork"].astype("category")
        data["Archived"] = data["Archived"].astype("category")
        data["Locked"] = data["Locked"].astype("category")

        data["CreatedDate"] = pd.to_datetime(data["CreatedDate"])
        data["LastPushedDate"] = pd.to_datetime(data["LastPushedDate"])

        return data

    # Appends new raw or pre-formatted data
    def append(self, new_data):
        self.__init__(new_data)
        self.data.drop_duplicates(subset=["ID"], inplace=True)
        self.data.to_pickle("Data/Data.pickle")
        print(f"\r{len(self.data)} now downloaded", end="", flush=True)
