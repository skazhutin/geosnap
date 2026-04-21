import unittest

from ml.ingestion.grid import build_grid, iter_moscow_tiles


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


if __name__ == "__main__":
    unittest.main()
