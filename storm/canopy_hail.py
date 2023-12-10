import requests

from turfpy.measurement import centroid, area
from states.state import get_state
from storm.wind_storm import WIND_THRESHOLD_ONE


def get_map_uuid(date: str) -> str:
    wind_map_url = f"https://api.canopyweather.com/hail-maps?max_convective_date={date}&min_convective_date={date}"
    headers = {
        "Authorization": "2e92be5b-c1e8-4ffa-8de5-3aaf9cfc1920",
        "Accept": "application/json"
    }

    resp = requests.get(wind_map_url, headers=headers)

    return resp.json()[0]['hail_map_uuid']


def get_hail_map(uuid: str):
    hail_map_url = f"https://api.canopyweather.com/hail-damage-geospatial/{uuid}"
    headers = {
        "Authorization": "2e92be5b-c1e8-4ffa-8de5-3aaf9cfc1920",
        "Accept": "application/geo+json"
    }

    resp = requests.get(hail_map_url, headers=headers)

    return resp.json()

