import json

from turfpy.measurement import boolean_point_in_polygon


state_polys_file = open('../data/geo/state_polys.json')
state_polys = json.load(state_polys_file)


state_mapping = {
    "arkansas": "AR",
    "georgia": "GA",
    "indiana": "IN",
    "illinois": "IL",
    "iowa": "IA",
    "kentucky": "KY",
    "tennessee": "TN",
    "missouri": "MO",
    "ohio": "OH"

}


curr_states = ['AR', 'GA', 'IN', 'IL', 'IA', 'KY', 'TN', 'MO', 'OH']


def get_state(point):
    """
    Function to get state of a certain point *only returns if state is within current states
    :param point: to get state of
    :return: state as state code
    """
    for state, state_poly in state_polys.items():
        if boolean_point_in_polygon(point, state_poly):
            try:
                state_code = state_mapping[state]
            except Exception as e:
                return None
            # Check if state is within current live states
            if state_code not in curr_states:
                return None

            return state_code

    return None


def get_curr_states() -> [str]:
    return curr_states
