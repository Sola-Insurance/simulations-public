import json
import random

from geojson import Feature, Point
from turfpy.measurement import bbox, boolean_point_in_polygon
from turfpy.random import random_position
from turfpy.transformation import circle

from geo import country_us

# These files represent assumptions that we gathered from previous hail storms
hail_damage_file = open('data/storms/hail/hail_damage.json')
hail_matrix_file = open('data/storms/hail/hail_matrix.json')
hail_size_file = open('data/storms/hail/hail_size_matrix.json')


hail_damage_matrix = json.load(hail_damage_file)
hail_state_matrix = json.load(hail_matrix_file)
hail_size_matrix = json.load(hail_size_file)


def generate_sample_hailstorm() -> dict:
    """
    Function that randomly generates stats for a synthetic hail storm using historical assumptions
    :return: dictionary with hail storm stats
    """
    dmg_choices = list(hail_damage_matrix.keys())
    dmg_weights = list(hail_damage_matrix.values())

    sim_dmg = random.choices(dmg_choices, k=1, weights=dmg_weights)[0]

    state_choices = list(hail_state_matrix[sim_dmg].keys())
    state_weights = list(hail_state_matrix[sim_dmg].values())

    sim_state = random.choices(state_choices, k=1, weights=state_weights)[0]

    size_choices = list(hail_size_matrix[sim_dmg].keys())
    size_weights = list(hail_size_matrix[sim_dmg].values())

    sim_size = random.choices(size_choices, k=1, weights=size_weights)[0]

    return {
        "severity": sim_dmg,
        "state": sim_state,
        "size": sim_size
    }


def generate_hail_polygon(storm_payouts: dict):
    """
    Function that generates hail shape file in geojson polygon format using hail statistics

    :param storm_payouts: Dict of str(severity) => int(payout) in USD.
    :return: hail shape file
    """
    hailstorm_data = generate_sample_hailstorm()

    state = hailstorm_data["state"]
    severity = hailstorm_data["severity"]
    size = hailstorm_data["size"]

    state_poly = country_us.state_polys[state]
    state_bbox = bbox(state_poly)

    us_feature = Feature(geometry=country_us.us_poly)

    hail_center = Feature(geometry=Point(coordinates=random_position(state_bbox)))

    if not boolean_point_in_polygon(hail_center, us_feature):
        return False

    hail_polygon = circle(hail_center, radius=float(size), steps=10, units='mi')

    hail_polygon["properties"] = {
        "payout": storm_payouts[severity],
        "severity": severity
    }

    return hail_polygon
