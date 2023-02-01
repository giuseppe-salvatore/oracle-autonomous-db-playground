from datetime import datetime


def time_diff(start_time: datetime, end_time: datetime) -> str:
    return str(end_time - start_time).split('.', 2)[0]


def to_userfriendly_str(time: str):
    tokens = time.split(':')
    final_str = ""
    if int(tokens[0]) > 0:
        final_str += str(int(tokens[0]))
        final_str += " hours "

    if int(tokens[1]) > 0:
        final_str += str(int(tokens[1]))
        final_str += " minutes "

    if (int(tokens[0]) > 0 or int(tokens[1]) > 0) and int(tokens[2]) > 0:
        final_str += "and "
        final_str += str(int(tokens[2]))
        final_str += " seconds "
    else:
        final_str += str(int(tokens[2]))
        final_str += " seconds "

    return final_str
