
WIND_THRESHOLD_ONE = 76
WIND_THRESHOLD_TWO = 111
WIND_THRESHOLD_THREE = 136
WIND_THRESHOLD_FOUR = 166
WIND_THRESHOLD_FIVE = 201


wind_speed_dict = {
    "0": 0,
    str(WIND_THRESHOLD_ONE): 0,
    str(WIND_THRESHOLD_TWO): 0,
    str(WIND_THRESHOLD_THREE): 0,
    str(WIND_THRESHOLD_FOUR): 0,
    str(WIND_THRESHOLD_FIVE): 0
}


wind_speed_dict_list = {
    "0": [],
    str(WIND_THRESHOLD_ONE): [],
    str(WIND_THRESHOLD_TWO): [],
    str(WIND_THRESHOLD_THREE): [],
    str(WIND_THRESHOLD_FOUR): [],
    str(WIND_THRESHOLD_FIVE): []
}


def get_wind_speed_dict_list() -> dict:
    return wind_speed_dict_list


def get_wind_speed_dict() -> dict:
    """
    Function to return wind speed dictionary
    :return:
    """
    return wind_speed_dict


def round_wind_speed(wind_speed: int) -> str:
    """
    Function that rounds wind speed down
    :param wind_speed: initial wind speed
    :return: rounded wind speed as string
    """
    if wind_speed <= WIND_THRESHOLD_ONE - 1:
        return "0"
    elif wind_speed <= WIND_THRESHOLD_TWO - 1:
        return str(WIND_THRESHOLD_ONE)
    elif wind_speed <= WIND_THRESHOLD_THREE - 1:
        return str(WIND_THRESHOLD_TWO)
    elif wind_speed <= WIND_THRESHOLD_FOUR - 1:
        return str(WIND_THRESHOLD_THREE)
    elif wind_speed <= WIND_THRESHOLD_FIVE - 1:
        return str(WIND_THRESHOLD_FOUR)
    else:
        return str(WIND_THRESHOLD_FIVE)
