import logging
import os
from dotenv.main import load_dotenv


class ConfigManager:
    """
    The config manager accesses environment variables and default variables and can therefore be used in
    local development mode as well as in dockerized production
    """

    def __init__(self, default_variables: dict = None, env_file: str = None):
        """

        :param env_file:
        """
        if default_variables is None:
            default_variables = {}
        self.default_variables = default_variables
        self.variables = {}
        if env_file is not None:
            if os.path.isfile(env_file):
                load_dotenv(env_file)
            else:
                logging.error("env file is not found")

    def get_value(self, key: str):
        if key in self.variables:
            return self.variables[key]
        elif key in os.environ:
            return os.environ[key]
        elif key in self.default_variables:
            return self.default_variables[key]
        else:
            raise KeyError("Key not present!")

    def overwride_value(self, key: str, value):
        self.variables[key] = value
