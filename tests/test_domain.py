"""Unit tests for common/domain.py — the deterministic household registry."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import math
import pytest
from common.domain import build_registry, solar_factor, demand_factor, ZONE_NAMES


class TestBuildRegistry:
    def test_correct_count(self):
        assert len(build_registry(10, 2)) == 10

    def test_deterministic(self):
        """Same args always produce the same registry."""
        assert build_registry(20, 3) == build_registry(20, 3)

    def test_zones_distributed(self):
        """Every zone gets at least one household (round-robin)."""
        registry = build_registry(8, 4)
        zones = {h.grid_zone for h in registry}
        assert len(zones) == 4

    def test_household_ids_unique(self):
        registry = build_registry(50, 4)
        ids = [h.household_id for h in registry]
        assert len(ids) == len(set(ids))

    def test_solar_capacity_non_negative(self):
        for h in build_registry(30, 3):
            assert h.solar_capacity_kw >= 0.0

    def test_base_load_positive(self):
        for h in build_registry(30, 3):
            assert h.base_load_kw > 0.0

    def test_num_zones_too_large_raises(self):
        with pytest.raises(ValueError):
            build_registry(5, len(ZONE_NAMES) + 1)


class TestSolarFactor:
    def test_zero_at_midnight(self):
        assert solar_factor(0.0, 0.0) == 0.0

    def test_zero_at_sunrise(self):
        assert solar_factor(6.0, 0.0) == 0.0

    def test_zero_at_sunset(self):
        assert solar_factor(18.0, 0.0) == 0.0

    def test_peak_near_noon(self):
        # Solar output should be highest around midday
        noon = solar_factor(12.0, 0.0)
        morning = solar_factor(8.0, 0.0)
        evening = solar_factor(16.0, 0.0)
        assert noon > morning
        assert noon > evening

    def test_full_cloud_reduces_output(self):
        clear = solar_factor(12.0, 0.0)
        cloudy = solar_factor(12.0, 100.0)
        assert cloudy < clear

    def test_output_between_0_and_1(self):
        for hour in range(25):
            for cloud in [0.0, 50.0, 100.0]:
                val = solar_factor(float(hour), cloud)
                assert 0.0 <= val <= 1.0


class TestDemandFactor:
    def test_overnight_below_midday(self):
        night = demand_factor(2.0)
        midday = demand_factor(12.0)
        # Baseline at 2am should be lower than midday
        assert night < midday

    def test_evening_peak_higher_than_night(self):
        night = demand_factor(3.0)
        evening = demand_factor(19.5)
        assert evening > night

    def test_always_positive(self):
        for h in range(25):
            assert demand_factor(float(h)) > 0.0
