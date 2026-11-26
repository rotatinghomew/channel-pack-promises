"""Exact multi-channel product, publication and order commitment backend."""
from .channels import ChannelGateway, MemoryChannelGateway
from .errors import (
    AuthorizationError,
    ConflictError,
    ExchangeError,
    IdempotencyConflictError,
    NotFoundError,
    ValidationError,
)
from .service import ExchangeService


def create_service(samples_directory: str | None = None) -> ExchangeService:
    """Create an in-memory service, optionally loading a samples directory."""
    service = ExchangeService()
    if samples_directory:
        service.ingest_samples(samples_directory)
    return service

__all__ = [
    "ExchangeService",
    "create_service",
    "ChannelGateway",
    "MemoryChannelGateway",
    "ExchangeError",
    "ValidationError",
    "NotFoundError",
    "ConflictError",
    "AuthorizationError",
    "IdempotencyConflictError",
]
