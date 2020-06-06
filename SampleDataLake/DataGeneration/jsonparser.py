"""
----------------------------------------------------------------------------------------------------------
Description:

Usage: Json Parser to get column providers type

Author  : Usman Zahid
Release : 1
Modification Log:

This can be executed on local or as well as on any EC2 instance
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
30/03/2020        Usman Zahid        			            Initial draft.

-----------------------------------------------------------------------------------------------------------
"""
import json


class JsonParser:
    """
    This class is written to get json object
    """

    def __init__(self):
        self.path = 'config.json'
        return None

    def read_json(self, config_param):
        """
        This method is being used to parse the JSON against the given table name as a key
        :param config_param: Table name
        :return: List of attributes
        """
        json_file = self.path
        with open(json_file) as f:
            data = json.load(f)
        param_list = []
        json_content = json.dumps(data)
        # print(type(json_content))
        json_1 = json.loads(json_content)
        for value in json_1[config_param].values():
            for i in value.values():
                param_list.append(i)
        return param_list
