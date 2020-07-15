import json
import pandas as pd

class Parser():
    data = pd.DataFrame(columns=["Name", "ID", "Description", "ForkCount", "IsFork", "Archived", "Locked", "CreatedDate", "LastPushedDate", "PrimaryLanguage", "Users", "Stars", "Watchs", "Issues", "PullRequests", "Labels", "Topics", "License", "Commits", "README"])

    def __init__(self, data=None):
        if type(data) == pd.DataFrame:
            self.data = self.data.append(data, ignore_index=True)
        elif data != None:
            for repo in data:
                self.data = self.data.append(self.remap_json(repo["node"]), ignore_index=True)

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

    # Appends new raw or pre-formatted data
    def append(self, new_data):
        if type(new_data) == pd.DataFrame:
            self.data = self.data.append(new_data, ignore_index=True)
        else:
            self.__init__(new_data)

        self.data.drop_duplicates(subset=["ID"], inplace=True)
