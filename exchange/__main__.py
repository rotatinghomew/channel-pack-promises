"""Command line entry point.

Examples:
  python -m exchange load-samples --samples samples --state /tmp/exchange.json
  python -m exchange serve --host 127.0.0.1 --port 8080 --state /tmp/exchange.json
  python -m exchange scenario --samples samples
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .httpapi import create_server
from .service import ExchangeService


def _service(state: str | None) -> ExchangeService:
    return ExchangeService.load_state(state) if state else ExchangeService()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="exchange", description=__doc__)
    parser.add_argument("--state", help="JSON state file for durable repeated integrations")
    sub = parser.add_subparsers(dest="command", required=True)

    load = sub.add_parser("load-samples")
    load.add_argument("--samples", default="samples")

    ingest = sub.add_parser("ingest")
    ingest.add_argument("kind")
    ingest.add_argument("path")

    sub.add_parser("scenario")

    serve = sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    explain = sub.add_parser("explain")
    explain.add_argument("--store")
    explain.add_argument("--barcode")
    explain.add_argument("--local")
    explain.add_argument("--channel")
    explain.add_argument("--listing")
    explain.add_argument("--push")
    explain.add_argument("--at")

    args = parser.parse_args(argv)
    service = _service(args.state)

    if args.command == "load-samples":
        result = service.ingest_samples(args.samples)
        if args.state:
            service.save_state(args.state)
        print(json.dumps({k: len(v) for k, v in result.items()}, ensure_ascii=False, indent=2))
        return 0
    if args.command == "ingest":
        results = service.ingest_file(args.path, args.kind)
        if args.state:
            service.save_state(args.state)
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return 0
    if args.command == "scenario":
        samples = Path(__file__).resolve().parents[1] / "samples"
        service.ingest_samples(samples)
        out = {
            "conflicts": service.list_conflicts(),
            "preview_2026_11_27_19_45": service.preview_channel(at="2026-11-27T19:45:00+08:00"),
            "order_17_trace": service.trace_order("order-17"),
            "order_18_trace": service.trace_order("order-18"),
            "order_21_trace": service.trace_order("order-21"),
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0
    if args.command == "explain":
        result = service.explain(
            store=args.store,
            barcode=args.barcode,
            local=args.local,
            channel=args.channel,
            listing=args.listing,
            push=args.push,
            at=args.at,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.command == "serve":
        if args.state:
            service.save_state(args.state) if not Path(args.state).exists() else None
        server = create_server(args.host, args.port, service=service, state_path=args.state)
        print(f"exchange backend listening on http://{args.host}:{args.port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
