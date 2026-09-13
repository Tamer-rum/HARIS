"""Storm map rows come from cycle evidence; the geography is configuration only."""

import unittest

import app


class StormMapTests(unittest.TestCase):
    def setUp(self):
        self.area = dict(app.DEMO_STORM_AREA)
        self.devices = [
            {"device_id": "ambulance-01", "tier": 1, "cell_id": "T03"},
            {"device_id": "sensor-01", "tier": 3, "cell_id": "T03"},
            {"device_id": "ghost-01", "tier": 1, "cell_id": "T05"},
        ]
        self.locations = [
            {"device_id": "ambulance-01", "latitude": 31.96, "longitude": 35.91},
            {"device_id": "sensor-01", "latitude": "31.97", "longitude": 35.90},
        ]

    def test_tower_colours_follow_congestion_evidence_and_never_guess(self):
        rows = app.storm_map_data({"T03": "High", "T01": "Low", "T04": "bogus"}, [], [], set(), self.area)
        towers = {t["cell"]: t for t in rows["towers"]}
        self.assertEqual(set(towers), set(app.CELL_SITES))
        self.assertEqual(towers["T03"]["colour"][:3], app.LEVEL_RGB["High"])
        self.assertEqual(towers["T01"]["colour"][:3], app.LEVEL_RGB["Low"])
        for missing in ("T02", "T04"):
            self.assertEqual(towers[missing]["level"], "N/A")
            self.assertEqual(towers[missing]["colour"][:3], app.UNKNOWN_RGB)
        self.assertEqual(len(app.storm_map_deck(rows).layers), 6)

    def test_only_located_devices_are_drawn_and_only_protected_ones_are_ringed(self):
        rows = app.storm_map_data({}, self.devices, self.locations, {"ambulance-01"}, self.area)
        self.assertEqual([d["device"] for d in rows["devices"]], ["ambulance-01", "sensor-01"])
        self.assertEqual([d["device"] for d in rows["rings"]], ["ambulance-01"])
        self.assertIn("PROTECTED", rows["rings"][0]["tip"])

    def test_storm_zone_is_labelled_simulated_and_configurable(self):
        self.assertEqual(app.storm_area()["radius_m"], app.DEMO_STORM_AREA["radius_m"])
        rows = app.storm_map_data({}, [], [], set(), {"latitude": 1.0, "longitude": 2.0, "radius_m": 5000.0})
        self.assertEqual(rows["zone"][0]["radius"], 5000.0)
        self.assertIn("SIMULATED", rows["zone"][0]["tip"])


if __name__ == "__main__":
    unittest.main()
