"""核对交换资料的客户编号、时序和关联条件。"""
import json
from pathlib import Path
import unittest


SAMPLES = Path(__file__).resolve().parents[1] / "samples"


def rows(name):
    content = (SAMPLES / f"{name}.ndjson").read_text(encoding="utf-8")
    return [json.loads(line) for line in content.splitlines()]


class ExchangeSampleTest(unittest.TestCase):
    def test_records_are_json_objects(self):
        files = sorted(SAMPLES.glob("*.ndjson"))
        self.assertEqual(len(files), 6)
        for path in files:
            with self.subTest(path=path.name):
                records = rows(path.stem)
                self.assertTrue(records)
                self.assertTrue(all(isinstance(row, dict) for row in records))

    def test_related_records_preserve_source_evidence(self):
        old, new, other = rows("catalog")
        self.assertEqual(old["barcode"], new["barcode"])
        self.assertEqual(old["until"], new["from"])
        self.assertNotEqual(new["pieces"], other["pieces"])
        first = rows("amendments")[0]["changes"][0]
        self.assertLess(first["from"], old["until"])
        receipts = rows("channel_receipts")
        self.assertEqual(receipts[0], receipts[-1])
        self.assertEqual(receipts[0]["listing"], receipts[1]["listing"])
        self.assertNotEqual(receipts[0]["channel"], receipts[1]["channel"])
