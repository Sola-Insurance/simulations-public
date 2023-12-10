import json

#These files contain bounding box information for the states and the US (lat long coords)
state_polys_file = open('data/geo/state_polys.json')
us_poly_file = open('data/geo/us_poly.json')

state_polys = json.load(state_polys_file)
us_poly = json.load(us_poly_file)
