"""Unit tests for the speed layer's pure-function business logic.

These tests require no Spark, no Kafka, and no database. They verify:
  - _evaluate_alerts: threshold rules produce correct alert types/severities
  - _zone_rows_from_batch: de-duplication and renewable-% calculation are correct

Run with:
    pytest tests/ -v
"""
import sys
import os

# Allow imports from the project root without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Helpers that let us import speed_layer without a running Spark session
# ---------------------------------------------------------------------------

def _make_zone_row(
    zone="ZONE-NORTH",
    window_start=None,
    window_end=None,
    sim_day=None,
    consumption=10.0,
    solar=4.0,
    net=6.0,
    renewable_pct=40.0,
    active_meters=5,
    avg_voltage=230.0,
    reading_count=10,
):
    """Build a zone-load tuple in the format _evaluate_alerts expects."""
    from datetime import datetime, date
    ws = window_start or datetime(2026, 1, 1, 8, 0)
    we = window_end or datetime(2026, 1, 1, 9, 0)
    sd = sim_day or date(2026, 1, 1)
    return (zone, ws, we, sd, consumption, solar, net, renewable_pct, active_meters, avg_voltage, reading_count)


# ---------------------------------------------------------------------------
# Import the pure functions under test
# ---------------------------------------------------------------------------

# Patch PySpark so we can import speed_layer without a Spark session
import unittest.mock as mock

pyspark_mock = mock.MagicMock()
sys.modules.setdefault("pyspark", pyspark_mock)
sys.modules.setdefault("pyspark.sql", pyspark_mock.sql)
sys.modules.setdefault("pyspark.sql.functions", pyspark_mock.sql.functions)
sys.modules.setdefault("pyspark.sql.types", pyspark_mock.sql.types)
sys.modules.setdefault("pyspark.sql.streaming", pyspark_mock.sql.streaming)

# Patch common.db and common.metrics to avoid needing live connections
sys.modules.setdefault("common.db", mock.MagicMock())
sys.modules.setdefault("common.metrics", mock.MagicMock())

from streaming.speed_layer import _evaluate_alerts, _zone_rows_from_batch  # noqa: E402


# ===========================================================================
# Tests for _evaluate_alerts
# ===========================================================================

class TestEvaluateAlerts:
    """Threshold rules over zone-load rows."""

    def test_no_alerts_for_healthy_zone(self):
        """A zone with good renewable % and normal load should produce no alerts."""
        row = _make_zone_row(consumption=10.0, solar=5.0, renewable_pct=50.0, active_meters=5)
        alerts = _evaluate_alerts([row])
        assert alerts == []

    def test_low_renewable_alert_fires(self):
        """renewable_pct below threshold → LOW_RENEWABLE_CONTRIBUTION warning."""
        row = _make_zone_row(consumption=10.0, solar=0.5, renewable_pct=5.0, active_meters=5)
        alerts = _evaluate_alerts([row])
        types = [a[0] for a in alerts]
        assert "LOW_RENEWABLE_CONTRIBUTION" in types
        alert = next(a for a in alerts if a[0] == "LOW_RENEWABLE_CONTRIBUTION")
        assert alert[1] == "WARNING"

    def test_low_renewable_alert_not_fired_when_zero_consumption(self):
        """If consumption is 0 (e.g. nighttime with no load at all), do not alert."""
        row = _make_zone_row(consumption=0.0, solar=0.0, renewable_pct=0.0, active_meters=5)
        alerts = _evaluate_alerts([row])
        types = [a[0] for a in alerts]
        assert "LOW_RENEWABLE_CONTRIBUTION" not in types

    def test_zone_overload_alert_fires(self):
        """Consumption above zone_overload_kwh → ZONE_OVERLOAD critical."""
        # Default threshold is 45.0 kWh per window (from settings).
        row = _make_zone_row(consumption=50.0, solar=5.0, renewable_pct=10.0, active_meters=5)
        alerts = _evaluate_alerts([row])
        types = [a[0] for a in alerts]
        assert "ZONE_OVERLOAD" in types
        alert = next(a for a in alerts if a[0] == "ZONE_OVERLOAD")
        assert alert[1] == "CRITICAL"

    def test_zone_silent_alert_fires_when_no_meters(self):
        """active_meters == 0 → ZONE_SILENT critical."""
        row = _make_zone_row(consumption=0.0, solar=0.0, renewable_pct=0.0, active_meters=0)
        alerts = _evaluate_alerts([row])
        types = [a[0] for a in alerts]
        assert "ZONE_SILENT" in types
        alert = next(a for a in alerts if a[0] == "ZONE_SILENT")
        assert alert[1] == "CRITICAL"

    def test_multiple_alerts_can_fire_simultaneously(self):
        """A zone with zero renewable AND overload should raise both alerts."""
        row = _make_zone_row(consumption=60.0, solar=0.0, renewable_pct=0.0, active_meters=3)
        alerts = _evaluate_alerts([row])
        types = [a[0] for a in alerts]
        assert "LOW_RENEWABLE_CONTRIBUTION" in types
        assert "ZONE_OVERLOAD" in types

    def test_alert_entity_id_matches_zone(self):
        """entity_id in the alert tuple should equal the zone name."""
        zone = "ZONE-EAST"
        row = _make_zone_row(zone=zone, consumption=0.0, solar=0.0, renewable_pct=0.0, active_meters=0)
        alerts = _evaluate_alerts([row])
        assert all(a[3] == zone for a in alerts)

    def test_multiple_zones_each_evaluated(self):
        """Two rows from different zones are evaluated independently."""
        healthy = _make_zone_row(zone="ZONE-NORTH", renewable_pct=50.0, active_meters=5)
        sick = _make_zone_row(zone="ZONE-SOUTH", renewable_pct=5.0, active_meters=0)
        alerts = _evaluate_alerts([healthy, sick])
        alert_zones = {a[3] for a in alerts}
        assert "ZONE-NORTH" not in alert_zones   # healthy zone: no alerts
        assert "ZONE-SOUTH" in alert_zones        # sick zone: alerts expected


# ===========================================================================
# Tests for _zone_rows_from_batch
# ===========================================================================

class TestZoneRowsFromBatch:
    """De-duplication and renewable-percentage arithmetic."""

    def _make_batch_row(self, zone="ZONE-NORTH", consumption=10.0, solar=4.0,
                        reading_count=10, event_ids=None, avg_voltage=230.0,
                        window_start=None, window_end=None, sim_day=None):
        from datetime import datetime, date
        return {
            "grid_zone": zone,
            "window_start": window_start or datetime(2026, 1, 1, 8, 0),
            "window_end": window_end or datetime(2026, 1, 1, 9, 0),
            "sim_day": sim_day or date(2026, 1, 1),
            "total_consumption_kwh": consumption,
            "total_solar_kwh": solar,
            "active_meters": 5,
            "avg_voltage": avg_voltage,
            "reading_count": reading_count,
            "event_ids": event_ids if event_ids is not None else [f"ev{i}" for i in range(reading_count)],
        }

    def test_renewable_pct_calculation(self):
        """renewable_pct = solar / consumption * 100."""
        row = self._make_batch_row(consumption=10.0, solar=4.0)
        result = _zone_rows_from_batch([row])
        assert len(result) == 1
        # renewable_pct is index 7
        assert abs(result[0][7] - 40.0) < 0.01

    def test_zero_consumption_gives_zero_renewable_pct(self):
        """No division by zero when consumption is 0."""
        row = self._make_batch_row(consumption=0.0, solar=0.0)
        result = _zone_rows_from_batch([row])
        assert result[0][7] == 0.0

    def test_duplicate_deflation(self):
        """When event_ids (unique) < reading_count, consumption/solar are scaled down."""
        # 10 readings but only 8 distinct event_ids → 20% duplicates
        row = self._make_batch_row(
            consumption=10.0, solar=4.0,
            reading_count=10,
            event_ids=[f"ev{i}" for i in range(8)],  # 8 distinct
        )
        result = _zone_rows_from_batch([row])
        consumption = result[0][4]
        solar = result[0][5]
        # Expected: 10 * (1 - 0.2) = 8.0; 4 * 0.8 = 3.2
        assert abs(consumption - 8.0) < 0.01
        assert abs(solar - 3.2) < 0.01

    def test_no_deflation_when_no_duplicates(self):
        """When all event_ids are distinct, values are unchanged."""
        row = self._make_batch_row(
            consumption=10.0, solar=4.0,
            reading_count=5,
            event_ids=["a", "b", "c", "d", "e"],
        )
        result = _zone_rows_from_batch([row])
        assert abs(result[0][4] - 10.0) < 0.001
        assert abs(result[0][5] - 4.0) < 0.001

    def test_net_import_is_consumption_minus_solar(self):
        """net_import_kwh = max(0, consumption - solar)."""
        row = self._make_batch_row(consumption=10.0, solar=4.0)
        result = _zone_rows_from_batch([row])
        net = result[0][6]
        assert abs(net - 6.0) < 0.001

    def test_net_import_floored_at_zero(self):
        """Solar exceeding consumption doesn't produce negative net import."""
        row = self._make_batch_row(consumption=3.0, solar=8.0)
        result = _zone_rows_from_batch([row])
        net = result[0][6]
        assert net == 0.0

    def test_multiple_rows_processed(self):
        """Each row in the batch is turned into one output row."""
        rows = [
            self._make_batch_row(zone="ZONE-NORTH"),
            self._make_batch_row(zone="ZONE-SOUTH"),
        ]
        result = _zone_rows_from_batch(rows)
        assert len(result) == 2
        zones = {r[0] for r in result}
        assert zones == {"ZONE-NORTH", "ZONE-SOUTH"}

    def test_empty_event_ids_uses_reading_count(self):
        """If event_ids is None or empty, fall back to reading_count with no deflation."""
        row = self._make_batch_row(consumption=5.0, solar=2.0, reading_count=3, event_ids=None)
        result = _zone_rows_from_batch([row])
        # No deflation: consumption should remain 5.0
        assert abs(result[0][4] - 5.0) < 0.001
