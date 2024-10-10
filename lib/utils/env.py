import os
from lib.utils.logger import log


def read_environment_variable(var_name: str):
    log.debug("Reading {} from env".format(var_name))
    tmpvar = os.getenv(var_name)
    if tmpvar is None:
        raise ImportError("Environment variable {} not set".format(var_name))

    return tmpvar
