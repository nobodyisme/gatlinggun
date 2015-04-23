from ConfigParser import ConfigParser, DEFAULTSECT
import json
import re


class JsonConfig(dict):
    def __init__(self, filename):
        self.update(json.load(open(filename)))
