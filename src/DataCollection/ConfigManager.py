import json

class ConfigManager():
    def get_config(self, nature=None):
        if nature == "config": return json.load(self.config_path)
        if nature == "history": return json.load(self.history_path)
        
        return json.load(self.config_path), json.load(self.history_path)

    def update(self, arguments: dict, root: dict):
        root.update(arguments)
        return root

    def save(self, json_file: dict, path: str):
        json.dump(json_file, open(path, "w"))
