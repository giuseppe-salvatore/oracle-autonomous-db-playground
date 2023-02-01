import os


def read_environment_variable(var_name: str):
    print("Reading {} from env".format(var_name))
    tmpvar = os.getenv(var_name)
    if tmpvar is None:
        raise ImportError("Environment variable {} not set".format(var_name))

    return tmpvar
