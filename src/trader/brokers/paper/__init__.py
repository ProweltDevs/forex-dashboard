"""Paper broker: simulated fills against live (or fake) ticks. Seed of the future backtest simulator."""

from .adapter import PaperBroker
from .settings import PaperSettings
from .source import FakeTickSource

__all__ = ["FakeTickSource", "PaperBroker", "PaperSettings"]
