import requests

from turfpy.measurement import centroid, area
from states.state import get_state
from storm.wind_storm import WIND_THRESHOLD_ONE


def get_map_uuid(date: str) -> str:
    wind_map_url = f"https://api.canopyweather.com/wind-maps?max_convective_date={date}&min_convective_date={date}"
    headers = {
        "Authorization": "2e92be5b-c1e8-4ffa-8de5-3aaf9cfc1920",
        "Accept": "application/json"
    }

    resp = requests.get(wind_map_url, headers=headers)

    return resp.json()[0]['wind_map_uuid']


def get_wind_map(uuid: str):
    wind_map_url = f"https://api.canopyweather.com/wind-speed-geospatial/{uuid}"
    headers = {
        "Authorization": "2e92be5b-c1e8-4ffa-8de5-3aaf9cfc1920",
        "Accept": "application/geo+json"
    }

    resp = requests.get(wind_map_url, headers=headers)

    return resp.json()


def get_wind_speeds(wind_fc) -> []:
    wind_features = wind_fc['features']
    wind_speeds = []

    for wind_feature in wind_features:
        center = centroid(wind_feature)
        state = get_state(center)
        storm_area = area(wind_feature)

        if state and int(wind_feature['properties']['speed_mph']) >= WIND_THRESHOLD_ONE:
            wind_speeds.append({
                "speed": int(wind_feature['properties']['speed_mph']),
                "state": state,
                "area": storm_area
            })

            print(storm_area/2589988.1)
            print(wind_feature)
            print(state)


            # if int(wind_feature['properties']['speed_mph']) >= WIND_THRESHOLD_ONE:
            #     print(storm_area)

    return wind_speeds
