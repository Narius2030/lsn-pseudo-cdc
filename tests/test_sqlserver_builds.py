from __future__ import annotations

import unittest

from sqlserver_cdc_s3.versioning import classify_sqlserver_2014_sp1_build


class SQLServer2014SP1BuildTests(unittest.TestCase):
    def test_cu10_build_is_recognized(self) -> None:
        result = classify_sqlserver_2014_sp1_build((12, 0, 4491, 0))
        self.assertIsNotNone(result)
        self.assertEqual(result["cu_label"], "SP1 CU10")
        self.assertEqual(result["kb"], "KB3204399")

    def test_cu13_doc_build_is_recognized(self) -> None:
        result = classify_sqlserver_2014_sp1_build((12, 0, 4520, 0))
        self.assertIsNotNone(result)
        self.assertEqual(result["cu_label"], "SP1 CU13")
        self.assertEqual(result["kb"], "KB4019099")

    def test_cu13_package_build_is_recognized(self) -> None:
        result = classify_sqlserver_2014_sp1_build((12, 0, 4522, 0))
        self.assertIsNotNone(result)
        self.assertEqual(result["cu_label"], "SP1 CU13")
        self.assertEqual(result["kb"], "KB4019099")


if __name__ == "__main__":
    unittest.main()
