"""Top-level alias for code that imports ``service`` directly."""
from exchange.errors import *  # noqa: F403
from exchange.service import ExchangeService

__all__ = ["ExchangeService"]
