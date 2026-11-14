"""Send timestamped sensor readings to the EF101-P20 TCP server."""

import argparse
import json
import random
import socket
import sys
import time
from datetime import datetime, timezone


def build_record(record_id: int) -> dict[str, object]:
    """Create one structured sensor-like reading."""
    return {
        "record_id": record_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "temperature_c": round(random.uniform(18.0, 30.0), 2),
        "voltage_v": round(random.uniform(3.0, 5.0), 3),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send sample sensor readings to the EF101-P20 TCP server."
    )
    parser.add_argument("--host", default="127.0.0.1", help="server IP or hostname")
    parser.add_argument("--port", type=int, default=5000, help="server TCP port")
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="seconds between records (default: 2)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="number of records to send; 0 sends until interrupted",
    )
    args = parser.parse_args()

    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    if args.interval <= 0:
        parser.error("--interval must be greater than 0")
    if args.count < 0:
        parser.error("--count cannot be negative")

    return args


def main() -> int:
    args = parse_args()
    address = (args.host, args.port)
    print(f"Connecting to {args.host}:{args.port} ...", flush=True)

    try:
        with socket.create_connection(address, timeout=5) as connection:
            print("Connected. Press Ctrl+C to stop sending.", flush=True)
            record_id = 1

            while args.count == 0 or record_id <= args.count:
                record = build_record(record_id)
                message = json.dumps(record, separators=(",", ":")) + "\n"
                connection.sendall(message.encode("utf-8"))

                print(
                    f"Sent #{record_id}: {record['timestamp']} | "
                    f"{record['temperature_c']} C | {record['voltage_v']} V",
                    flush=True,
                )

                record_id += 1
                if args.count == 0 or record_id <= args.count:
                    time.sleep(args.interval)

    except (ConnectionRefusedError, TimeoutError, socket.timeout) as error:
        print(
            f"Connection failed: could not reach {args.host}:{args.port} "
            f"({error}). Check that the server is running and the address/port are correct.",
            file=sys.stderr,
        )
        return 1
    except OSError as error:
        print(
            f"Network connection to {args.host}:{args.port} was lost: {error}",
            file=sys.stderr,
        )
        return 1
    except KeyboardInterrupt:
        print("\nClient stopped.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())