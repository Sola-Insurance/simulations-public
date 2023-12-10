import unittest

from db.v2 import geo_types


class GeoTypesTestCase(unittest.TestCase):
    def test_str_to_int(self):
        for geo_type_str, expected_value in geo_types.GEO_TYPE_MAPPINGS.items():
            self.assertEqual(expected_value, geo_types.str_to_int(geo_type_str))

    def test_int_to_str(self):
        for expected_value, geo_type_int in geo_types.GEO_TYPE_MAPPINGS.items():
            self.assertEqual(expected_value, geo_types.int_to_str(geo_type_int))

    def test_unknown_str(self):
        self.assertEqual(geo_types.GEO_TYPE_UNKNOWN, geo_types.str_to_int('bad value'))

    def test_unknown_int(self):
        self.assertEqual(geo_types.GEO_TYPE_UNKNOWN_STR, geo_types.int_to_str(-238392))


if __name__ == '__main__':
    unittest.main()
