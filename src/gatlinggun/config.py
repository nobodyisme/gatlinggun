import json


class JsonConfig(dict):
    def __init__(self, filename):
        self.update(json.loads(open(filename).read()))
