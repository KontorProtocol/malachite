import unittest

from semver_release_type import release_type


class ReleaseTypeTests(unittest.TestCase):
    def test_numeric_bumps_and_prerelease_identity(self):
        cases = [
            ("0.8.2", "0.9.0-kontor.1", "major"),
            ("0.9.0-kontor.1", "0.9.0-kontor.1", "minor"),
            ("0.9.0-kontor.1", "0.9.0-kontor.2", "minor"),
            ("0.9.0-kontor.1", "0.9.0", "minor"),
            ("0.9.0-kontor.1", "0.9.0-kontor.1+build.2", "minor"),
            ("0.9.0-kontor.1", "0.9.1-kontor.1", "minor"),
            ("0.9.1-kontor.1", "0.10.0-kontor.1", "major"),
            ("0.0.1-kontor.1", "0.0.2-kontor.1", "major"),
            ("0.9.0-kontor.1", "1.0.0-kontor.1", "major"),
            ("1.0.0-kontor.1", "1.1.0-kontor.1", "minor"),
            ("1.0.0-kontor.1", "1.0.1-kontor.1", "patch"),
        ]
        for baseline, current, expected in cases:
            with self.subTest(baseline=baseline, current=current):
                self.assertEqual(release_type(baseline, current), expected)

    def test_rejects_regression_and_invalid_version(self):
        for baseline, current in [("0.9.0", "0.8.9"), ("1.0.0", "1.0"), ("1.0.0", "bad")]:
            with self.subTest(baseline=baseline, current=current):
                with self.assertRaises(ValueError):
                    release_type(baseline, current)
