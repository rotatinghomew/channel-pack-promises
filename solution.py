"""Compatibility entrypoint for integrations that expect ``solution.py``."""
from pathlib import Path

from exchange import (
    AuthorizationError,
    ConflictError,
    ExchangeError,
    ExchangeService,
    IdempotencyConflictError,
    NotFoundError,
    ValidationError,
)

__all__ = [
    "ExchangeService",
    "create_service",
    "ExchangeError",
    "ValidationError",
    "NotFoundError",
    "ConflictError",
    "AuthorizationError",
    "IdempotencyConflictError",
]


def create_service(state: dict | str | Path | None = None) -> ExchangeService:
    if state is None:
        return ExchangeService()
    if isinstance(state, dict):
        return ExchangeService(state)
    path = Path(state)
    if path.is_file():
        return ExchangeService.load_state(path)
    service = ExchangeService()
    service.ingest_samples(path)
    return service
