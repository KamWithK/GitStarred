import re
import asyncpg

from datetime import datetime

class Parser():
    # Avoid TypeErrors due to None types within nested dictionaries
    def safe_parse_list(self, json, keys: list):
        entry = json

        for key in keys:
            try: entry = entry[key]
            except: return

        return entry

    # Avoid TypeErrors due to None types within many nested dictionaries
    def safe_parse(self, json, values: dict):
        entry = {}

        for field, keys in values.items():
            entry[field] = self.safe_parse_list(json, keys)

        return entry

    def safe_covert(self, data, converter, default):
        try:
            return converter(data)
        except:
            return default

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
        data["ForkCount"] = self.safe_covert(data["ForkCount"], int, 0)
        data["Users"] = self.safe_covert(data["Users"], int, 1)
        data["Stars"] = self.safe_covert(data["Stars"], int, 0)
        data["Watchs"] = self.safe_covert(data["Watchs"], int, 1)
        data["Issues"] = self.safe_covert(data["Issues"], int, 0)
        data["PullRequests"] = self.safe_covert(data["PullRequests"], int, 0)
        data["Commits"] = self.safe_covert(data["Commits"], int, 0)

        data["Fork"] = self.safe_covert(data["Fork"], bool, False)
        data["Archived"] = self.safe_covert(data["Archived"], bool, False)
        data["Locked"] = self.safe_covert(data["Locked"], bool, False)

        data["Topics"] = ", ".join(data["Topics"])

        if data["Name"] == "": data["Name"] = None
        if data["ID"] == "": data["ID"] = None
        if data["Description"] == "": data["Description"] = None
        if data["Topics"] == "": data["Topics"] = None
        if data["License"] == "": data["License"] = None
        if data["README"] == "": data["README"] = None
        if data["PrimaryLanguage"] == "": data["PrimaryLanguage"] = None

        if data["ID"] != None: data["ID"] = re.sub("\u0000", "", data["ID"])
        if data["Name"] != None: data["Name"] = re.sub("\u0000", "", data["Name"])
        if data["Description"] != None: data["Description"] = re.sub("\u0000", "", data["Description"])
        if data["Topics"] != None: data["Topics"] = re.sub("\u0000", "", data["Topics"])
        if data["License"] != None: data["License"] = re.sub("\u0000", "", data["License"])
        if data["README"] != None: data["README"] = re.sub("\u0000", "", data["README"])
        if data["PrimaryLanguage"] != None: data["PrimaryLanguage"] = re.sub("\u0000", "", data["PrimaryLanguage"])

        if data["CreatedDate"] != None: data["CreatedDate"] = datetime.strptime(data["CreatedDate"], "%Y-%m-%dT%H:%M:%SZ")
        if data["LastPushedDate"] != None: data["LastPushedDate"] = datetime.strptime(data["LastPushedDate"], "%Y-%m-%dT%H:%M:%SZ")

        return (data["ID"], data["Name"], data["Description"], data["Topics"], data["License"], data["README"], data["PrimaryLanguage"], data["CreatedDate"], data["LastPushedDate"], data["Fork"], data["Archived"], data["Locked"], data["ForkCount"], data["Commits"], data["Issues"], data["PullRequests"], data["Users"], data["Watchs"], data["Stars"])

    async def commit_data(self, data: dict, connection: asyncpg.Connection):
        query = "INSERT INTO Repositories(ID, Name, Description, Topics, License, README, PrimaryLanguage, CreatedDate, LastPushedDate, Fork, Archived, Locked, ForkCount, Commits, Issues, PullRequests, Users, Watchs, Stars) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) ON CONFLICT (ID) DO UPDATE SET ID=$1, Name=$2, Description=$3, Topics=$4, License=$5, README=$6, PrimaryLanguage=$7, CreatedDate=$8, LastPushedDate=$9, Fork=$10, Archived=$11, Locked=$12, ForkCount=$13, Commits=$14, Issues=$15, PullRequests=$16, Users=$17, Watchs=$18, Stars=$19"
        parsed_entries = []

        for repo in data:
            temp = self.remap_json(repo["node"])
            parsed_entries.append(self.fix_dtypes(temp))

        await connection.executemany(query, parsed_entries)

        # print(f"\rSearched {await connection.fetchval('SELECT COUNT(*) FROM Repositories')} repositories", end="", flush=True)
