"""Errors with stable machine-readable codes for the exchange backend."""
from __future__ import annotations

from typing import Any


class ExchangeError(Exception):
    """Base class for all intentional domain/API errors."""

    code = "exchange_error"
    http_status = 400

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: dict[str, Any] | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        if http_status is not None:
            self.http_status = http_status
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


class ValidationError(ExchangeError):
    code = "validation_error"
    http_status = 400


class NotFoundError(ExchangeError):
    code = "not_found"
    http_status = 404


class AuthorizationError(ExchangeError):
    code = "operator_not_authorized"
    http_status = 403


class ConflictError(ExchangeError):
    code = "state_conflict"
    http_status = 409


class IdempotencyConflictError(ExchangeError):
    code = "idempotency_conflict"
    http_status = 409


class UnresolvedCommitmentError(ExchangeError):
    code = "unresolved_commitment"
    http_status = 409
