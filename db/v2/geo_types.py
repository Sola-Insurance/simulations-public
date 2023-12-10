

"""Defines the mapping of geography types (state, county, zip) to numeric representations.

We do this to optimize table size and make joins faster (ints are faster than strings).
See:
https://docs.google.com/document/d/1T1ILFRcgrEmGT6QCx6OmqEyT0efc7FC3tUWM_aPD0PU/edit#heading=h.uro8m94z32xl
"""


GEO_TYPE_UNKNOWN = -1  # Error case, if we introduce a new geo type.
# Note, we're keeping 0 reserved for now.
GEO_TYPE_STATE_VALUE = 1
GEO_TYPE_COUNTY_VALUE = 2
GEO_TYPE_ZIP_VALUE = 3
GEO_TYPE_TOTAL_VALUE = 4


GEO_TYPE_TOTAL_STR = 'total'
GEO_TYPE_STATE_STR = 'state'
GEO_TYPE_COUNTY_STR = 'county'
GEO_TYPE_ZIP_STR = 'zip'
GEO_TYPE_UNKNOWN_STR = '_unknown_geo_type_'


GEO_TYPE_MAPPINGS = {
    GEO_TYPE_TOTAL_STR: GEO_TYPE_TOTAL_VALUE,
    GEO_TYPE_STATE_STR: GEO_TYPE_STATE_VALUE,
    GEO_TYPE_COUNTY_STR: GEO_TYPE_COUNTY_VALUE,
    GEO_TYPE_ZIP_STR: GEO_TYPE_ZIP_VALUE,
    GEO_TYPE_UNKNOWN_STR: GEO_TYPE_UNKNOWN
}


def str_to_int(geo_type: str) -> int:
    """From a geo-type string return its integer value."""
    return GEO_TYPE_MAPPINGS.get(geo_type.strip().lower(), GEO_TYPE_UNKNOWN)


def int_to_str(geo_type_value: int) -> str:
    """From a geo-type int return its string value"""
    for geo_type_str, mapped_value in GEO_TYPE_MAPPINGS.items():
        if geo_type_value == mapped_value:
            return geo_type_str
    return GEO_TYPE_UNKNOWN_STR
