"""检查基础服务状态。"""

import unittest

from src.app import health_payload


class HealthTest(unittest.TestCase):
    def test_health_payload(self) -> None:
        self.assertEqual(health_payload(), {"status": "ok"})


if __name__ == "__main__":
    unittest.main()
