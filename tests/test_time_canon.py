"""时间与规范化摘要的基础行为。"""

import unittest

from src import canon
from src.timeutil import TimeError, parse_duration_seconds, parse_instant


class TimeTest(unittest.TestCase):
    def test_offset_preserved_across_midnight(self) -> None:
        start = parse_instant("2026-09-19T23:00:00+08:00")
        end_ts = start.unix_seconds + parse_duration_seconds("PT8H")
        from src.timeutil import Instant

        end = Instant(end_ts, start.tz_offset_minutes)
        self.assertEqual(end.iso_with_original_offset(), "2026-09-20T07:00:00+08:00")
        self.assertEqual(end.iso_utc(), "2026-09-19T23:00:00Z")

    def test_naive_time_rejected(self) -> None:
        with self.assertRaises(TimeError):
            parse_instant("2026-09-19T23:00:00")

    def test_zulu_suffix(self) -> None:
        instant = parse_instant("2026-09-19T15:00:00Z")
        self.assertEqual(instant.tz_offset_minutes, 0)
        self.assertEqual(instant.unix_seconds, parse_instant("2026-09-19T23:00:00+08:00").unix_seconds)

    def test_duration(self) -> None:
        self.assertEqual(parse_duration_seconds("PT1H30M"), 5400)
        self.assertEqual(parse_duration_seconds("P1DT12H"), 129600)
        with self.assertRaises(TimeError):
            parse_duration_seconds("PT-5M")


class CanonTest(unittest.TestCase):
    def test_digest_stable_independent_of_key_order(self) -> None:
        a = {"b": 1, "a": [1, 2, {"c": 3}]}
        b = {"a": [1, 2, {"c": 3}], "b": 1}
        self.assertEqual(canon.digest_payload(a), canon.digest_payload(b))

    def test_digest_detects_change(self) -> None:
        d1 = canon.digest_payload({"level": "黄色"})
        d2 = canon.digest_payload({"level": "红色"})
        self.assertNotEqual(d1, d2)
        self.assertTrue(canon.verify_digest({"level": "黄色"}, d1))
        self.assertFalse(canon.verify_digest({"level": "红色"}, d1))


if __name__ == "__main__":
    unittest.main()
