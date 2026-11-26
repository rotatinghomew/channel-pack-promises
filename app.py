"""Conventional local entrypoint: ``python app.py``."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchange.httpapi import create_server  # noqa: E402
from exchange.service import ExchangeService  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the exchange JSON HTTP backend")
    parser.add_argument("--host", default=os.environ.get("EXCHANGE_HOST") or os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("EXCHANGE_PORT") or os.environ.get("PORT", "8080")),
    )
    parser.add_argument("--state", default=os.environ.get("EXCHANGE_STATE") or os.environ.get("STATE_FILE"))
    parser.add_argument("--samples", default=os.environ.get("EXCHANGE_SAMPLES"))
    args = parser.parse_args()

    service = ExchangeService.load_state(args.state) if args.state else ExchangeService()
    if args.samples:
        service.ingest_samples(args.samples)
    server = create_server(args.host, args.port, service=service, state_path=args.state)
    print(f"exchange backend listening on http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

