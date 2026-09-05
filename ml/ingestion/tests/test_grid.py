import unittest

from ml.ingestion.grid import BBox, build_grid, iter_moscow_tiles


class GridTests(unittest.TestCase):
    def test_build_grid_splits_bbox(self) -> None:
        tiles = build_grid(
            min_lat=55.55,
            max_lat=55.57,
            min_lon=37.30,
            max_lon=37.32,
            lat_step=0.01,
            lon_step=0.01,
        )
        self.assertEqual(len(tiles), 4)
        self.assertAlmostEqual(tiles[0].min_lat, 55.55)
        self.assertAlmostEqual(tiles[0].max_lat, 55.56)

    def test_build_grid_rejects_large_tile_area(self) -> None:
        with self.assertRaises(ValueError):
            build_grid(
                min_lat=55.55,
                max_lat=55.95,
                min_lon=37.30,
                max_lon=37.90,
                lat_step=0.2,
                lon_step=0.2,
            )

    def test_iter_moscow_tiles_not_empty(self) -> None:
        tiles = list(iter_moscow_tiles())
        self.assertTrue(len(tiles) > 0)
        self.assertAlmostEqual(tiles[0].min_lat, 55.55)
        self.assertAlmostEqual(tiles[0].min_lon, 37.30)

    def test_tile_has_stable_key_center_and_covering_radius(self) -> None:
        tile = BBox(min_lat=55.55, max_lat=55.56, min_lon=37.30, max_lon=37.31)
        self.assertEqual(tile.center, (55.555, 37.305))
        self.assertEqual(tile.key, "55.5500000:55.5600000:37.3000000:37.3100000")
        self.assertGreater(tile.enclosing_radius_m(), 500)

    def test_invalid_world_bounds_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_grid(min_lat=-91, max_lat=-89, min_lon=0, max_lon=1)


if __name__ == "__main__":
    unittest.main()
