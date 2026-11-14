"""Receive newline-delimited JSON sensor records over TCP and save them to CSV."""

import argparse
import csv
import json
import math
import socketserver
import sys
import threading
from pathlib import Path
from typing import Any

CSV_FIELDS = ("record_id", "timestamp", "temperature_c", "voltage_v")


class RecordStore:
    """Thread-safe CSV writer and running statistics for received records."""

    def __init__(self, csv_path: Path) -> None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = csv_path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=CSV_FIELDS)
        self._lock = threading.Lock()
        self._count = 0
        self._temperature_total = 0.0
        self._voltage_total = 0.0

        if csv_path.stat().st_size == 0:
            self._writer.writeheader()
            self._file.flush()

    def add(self, record: dict[str, Any]) -> None:
        """Persist and print a validated record."""
        with self._lock:
            self._writer.writerow({field: record[field] for field in CSV_FIELDS})
            self._file.flush()
            self._count += 1
            self._temperature_total += record["temperature_c"]
            self._voltage_total += record["voltage_v"]

            print(
                f"{record['record_id']:>9}  {record['timestamp']:<25}  "
                f"{record['temperature_c']:>12.2f}  {record['voltage_v']:>9.3f}",
                flush=True,
            )

    def summary(self) -> str:
        with self._lock:
            if self._count == 0:
                return "No valid records received."

            return (
                f"Records received this run: {self._count}\n"
                f"Average temperature: {self._temperature_total / self._count:.2f} C\n"
                f"Average voltage: {self._voltage_total / self._count:.3f} V"
            )

    def close(self) -> None:
        with self._lock:
            self._file.close()


def validate_record(value: Any) -> dict[str, Any]:
    """Check the fields used by this demonstration before saving a record."""
    if not isinstance(value, dict):
        raise ValueError("record must be a JSON object")

    record_id = value.get("record_id")
    timestamp = value.get("timestamp")
    temperature = value.get("temperature_c")
    voltage = value.get("voltage_v")

    if isinstance(record_id, bool) or not isinstance(record_id, (int, str)):
        raise ValueError("record_id must be an integer or string")
    if not isinstance(timestamp, str) or not timestamp:
        raise ValueError("timestamp must be a non-empty string")
    if isinstance(temperature, bool) or not isinstance(temperature, (int, float)):
        raise ValueError("temperature_c must be numeric")
    if isinstance(voltage, bool) or not isinstance(voltage, (int, float)):
        raise ValueError("voltage_v must be numeric")
    if not math.isfinite(temperature) or not math.isfinite(voltage):
        raise ValueError("sensor readings must be finite numbers")

    return {
        "record_id": record_id,
        "timestamp": timestamp,
        "temperature_c": float(temperature),
        "voltage_v": float(voltage),
    }


class RecordHandler(socketserver.StreamRequestHandler):
    """Read complete JSON records until the client closes its connection."""

    def handle(self) -> None:
        client = f"{self.client_address[0]}:{self.client_address[1]}"
        print(f"Client connected: {client}", flush=True)
        print(
            f"{'ID':>9}  {'TIMESTAMP':<25}  {'TEMP (C)':>12}  {'VOLTAGE (V)':>9}",
            flush=True,
        )

        while True:
            try:
                line = self.rfile.readline()
            except OSError as error:
                print(f"Client {client} disconnected unexpectedly: {error}", flush=True)
                break

            if not line:
                break

            try:
                decoded = line.decode("utf-8")
                record = validate_record(json.loads(decoded))
                self.server.store.add(record)  # type: ignore[attr-defined]
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                print(f"Ignored invalid record from {client}: {error}", flush=True)

        print(f"Client disconnected: {client}", flush=True)


class DataServer(socketserver.ThreadingTCPServer):
    """Threaded TCP server so one client cannot block other connections."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple[str, int], store: RecordStore) -> None:
        self.store = store
        super().__init__(address, RecordHandler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Receive sensor readings and append them to a CSV file."
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="interface to listen on (default: all IPv4 interfaces)",
    )
    parser.add_argument("--port", type=int, default=5000, help="TCP port to listen on")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("records.csv"),
        help="output CSV path (default: records.csv)",
    )
    args = parser.parse_args()

    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")

    return args


def main() -> int:
    args = parse_args()
    store = RecordStore(args.csv)

    try:
        with DataServer((args.host, args.port), store) as server:
            actual_host, actual_port = server.server_address[:2]
            print(f"Listening on {actual_host}:{actual_port} (TCP)", flush=True)
            print(f"Saving records to: {args.csv.resolve()}", flush=True)
            print("Press Ctrl+C to stop the server.\n", flush=True)
            server.serve_forever()

    except OSError as error:
        print(
            f"Could not start server on {args.host}:{args.port}: {error}",
            file=sys.stderr,
        )
        return 1
    except KeyboardInterrupt:
        print("\nServer stopping.", flush=True)
    finally:
        print("\n--- Session summary ---", flush=True)
        print(store.summary(), flush=True)
        store.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())