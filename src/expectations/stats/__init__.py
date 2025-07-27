"""Collect and persist basic table statistics."""

from .collector import TableStatsCollector
from .models import MetricStat

__all__ = ["TableStatsCollector", "MetricStat"]
