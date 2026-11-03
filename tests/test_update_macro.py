import copy
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("update_macro", ROOT / "scripts/update_macro.py")
macro = importlib.util.module_from_spec(spec)
spec.loader.exec_module(macro)


class MacroDataTests(unittest.TestCase):
    def test_weekly_percentile_uses_three_calendar_years(self):
        dates = pd.date_range("2022-01-01", periods=210, freq="W")
        values = pd.Series([100.0] * 53 + [0.0] * 156 + [1.0], index=dates)
        self.assertEqual(macro.calc_stats(values)["percentile_3y"], 99)

    def test_fred_tga_and_wresbal_are_converted_to_billions_and_dated(self):
        data = json.loads((ROOT / "data/macro_daily.json").read_text(encoding="utf-8"))

        class FakeFred:
            def __init__(self, **kwargs):
                pass

            def get_series(self, code, **kwargs):
                idx = pd.to_datetime(["2026-10-21", "2026-10-28"])
                if code == "WTREGEN":
                    return pd.Series([950000.0, 999727.0], index=idx)
                if code == "WRESBAL":
                    return pd.Series([2962607.0, 2893770.0], index=idx)
                return pd.Series([4.0, 5.0], index=idx)

        with patch.object(macro, "Fred", FakeFred), patch.dict("os.environ", {"FRED_API_KEY": "test-only"}):
            result = macro.fetch_us_macro(copy.deepcopy(data["metrics"]["us"]))
        self.assertEqual(result["tga"]["value"], 999.73)
        self.assertEqual(result["tga"]["observation_date"], "2026-10-28")
        self.assertEqual(result["wresbal"]["value"], 2893.77)
        self.assertEqual(result["wresbal"]["observation_date"], "2026-10-28")

    def test_total_fred_failure_does_not_claim_update_success(self):
        data = json.loads((ROOT / "data/macro_daily.json").read_text(encoding="utf-8"))

        class BrokenFred:
            def __init__(self, **kwargs):
                pass

            def get_series(self, *args, **kwargs):
                raise RuntimeError("offline")

        with patch.object(macro, "Fred", BrokenFred), patch.dict("os.environ", {"FRED_API_KEY": "test-only"}):
            with self.assertRaises(RuntimeError):
                macro.fetch_us_macro(data["metrics"]["us"])

    def test_rule_engine_and_soft_reports_are_consistent(self):
        us = copy.deepcopy(macro.DEFAULT_US_METRICS)
        cn = copy.deepcopy(macro.DEFAULT_CN_METRICS)
        engine_out = macro.run_hard_rule_engine(us, cn)
        self.assertEqual(len(engine_out["hard_rules"]), 7)
        self.assertEqual(
            engine_out["barbell_allocation"]["defense_total"] + engine_out["barbell_allocation"]["offense_total"],
            100
        )
        reports = macro.generate_soft_reports(us, cn, engine_out)
        self.assertIn("边际回落", reports["trading_perspective"]["transmission_logic"])
        self.assertIn("R007-DR007", reports["wealth_perspective"]["core_marginal_change"])


if __name__ == "__main__":
    unittest.main()
