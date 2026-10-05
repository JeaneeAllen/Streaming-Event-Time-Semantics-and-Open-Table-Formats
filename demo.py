import csv
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import pyarrow as pa
except ModuleNotFoundError:
    pa = None

try:
    from deltalake import DeltaTable, write_deltalake
except ModuleNotFoundError:
    DeltaTable = None
    write_deltalake = None


def require_deltalake():
    if DeltaTable is None or write_deltalake is None:
        raise ModuleNotFoundError(
            "deltalake is required for this demo. "
            "Install it with: pip install deltalake"
        )


def require_pyarrow():
    if pa is None:
        raise ModuleNotFoundError(
            "pyarrow is required for this demo. "
            "Install it with: pip install pyarrow"
        )


ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"

WINDOW_MINUTES = 15
WATERMARK_MINUTES = 12
ALLOWED_MINUTES = 5

BASE = datetime(2026, 3, 1, tzinfo=timezone.utc)

RETENTION = {
    "delta.deletedFileRetentionDuration": "interval 7 days",
    "delta.logRetentionDuration": "interval 30 days",
}


def timestamp(value):
    return datetime.fromisoformat(value)


def window_start(value):
    return value.replace(
        minute=value.minute // WINDOW_MINUTES * WINDOW_MINUTES,
        second=0,
        microsecond=0,
    )


def group_key(event, field="event_time"):
    return (
        window_start(timestamp(event[field])).isoformat(),
        event["zone"],
    )


def checkpoint(message):
    print("\n" + message)
    input("Press Enter to continue...")


# ---------------------------------------------------------
# GENERATOR: five days, 1,000 trips per day.
# Each day has exactly 920 fast, 70 medium, and 10 tail trips.
# ---------------------------------------------------------
def generate_events():
    rng = random.Random(42)
    events = []

    for day in range(5):
        bands = ["fast"] * 920 + ["medium"] * 70 + ["tail"] * 10
        rng.shuffle(bands)

        for number, band in enumerate(bands):
            # Evening demand is higher than midday demand.
            hour = rng.choices(
                [8, 12, 17, 18],
                weights=[2, 1, 3, 4],
            )[0]

            event_time = BASE + timedelta(
                days=day,
                hours=hour,
                minutes=rng.randrange(60),
                seconds=rng.randrange(60),
            )

            # The longest delays concentrate in Harbor.
            zone = (
                "Harbor"
                if band == "tail"
                else rng.choices(
                    ["Harbor", "Downtown", "Airport"],
                    weights=[4, 4, 2],
                )[0]
            )

            if band == "fast":
                delay_seconds = rng.randint(0, 120)

            elif band == "medium":
                delay_seconds = rng.randint(121, 720)

            elif number % 2:
                delay_seconds = rng.randint(1200, 5400)

            else:
                next_morning = (
                    event_time.replace(hour=8, minute=0, second=0)
                    + timedelta(days=1)
                )
                delay_seconds = int(
                    (next_morning - event_time).total_seconds()
                )

            event = {
                "trip_id": f"D{day + 1}-{number:04}",
                "event_time": event_time.isoformat(),
                "processing_time": (
                    event_time + timedelta(seconds=delay_seconds)
                ).isoformat(),
                "zone": zone,
                "band": band,
            }

            # A new producer field appears on simulated day 4.
            if day >= 3:
                event["surge_multiplier"] = rng.choice(
                    [1.0, 1.25, 1.5, 2.0]
                )

            events.append(event)

    # Arrival order differs from trip-completion order.
    events.sort(
        key=lambda event: (
            event["processing_time"],
            event["trip_id"],
        )
    )

    return events


def raw_table(events, evolved):
    fields = [
        ("trip_id", pa.string()),
        ("event_time", pa.string()),
        ("processing_time", pa.string()),
        ("zone", pa.string()),
        ("band", pa.string()),
    ]

    if evolved:
        fields.append(("surge_multiplier", pa.float64()))

    return pa.Table.from_pylist(
        events,
        schema=pa.schema(fields),
    )


# ---------------------------------------------------------
# AGGREGATION: count trips by original event-time window.
# ---------------------------------------------------------
def aggregate_table(events, watermark, evolved):
    groups = defaultdict(list)

    for event in events:
        groups[group_key(event)].append(event)

    rows = []

    for (start, zone), trips in sorted(groups.items()):
        end = timestamp(start) + timedelta(minutes=WINDOW_MINUTES)

        if watermark < end:
            status = "open"
        elif watermark < end + timedelta(minutes=ALLOWED_MINUTES):
            status = "provisional"
        else:
            status = "closed"

        row = {
            "window_start": start,
            "zone": zone,
            "trip_count": len(trips),
            "status": status,
        }

        if evolved:
            values = [
                trip["surge_multiplier"]
                for trip in trips
                if trip.get("surge_multiplier") is not None
            ]

            row["surge_multiplier"] = (
                sum(values) / len(values) if values else None
            )

        rows.append(row)

    fields = [
        ("window_start", pa.string()),
        ("zone", pa.string()),
        ("trip_count", pa.int64()),
        ("status", pa.string()),
    ]

    if evolved:
        fields.append(("surge_multiplier", pa.float64()))

    return pa.Table.from_pylist(
        rows,
        schema=pa.schema(fields),
    )


def write_json_lines(path, events):
    path.write_text(
        "".join(json.dumps(event) + "\n" for event in events)
    )


# ---------------------------------------------------------
# COMPARISON: event-time counts beside processing-time counts.
# ---------------------------------------------------------
def show_comparison(accepted, all_events, side_output):
    event_counts = Counter(group_key(event) for event in accepted)

    processing_counts = Counter(
        group_key(event, "processing_time")
        for event in all_events
    )

    expected_counts = Counter(
        group_key(event) for event in all_events
    )

    side_counts = Counter(
        group_key(event) for event in side_output
    )

    rows = []

    for key in sorted(set(expected_counts) | set(processing_counts)):
        expected = expected_counts[key]

        rows.append({
            "window_start": key[0],
            "zone": key[1],
            "event_time_count": event_counts[key],
            "processing_time_count": processing_counts[key],
            "expected_count": expected,
            "completeness_pct": (
                round(100 * event_counts[key] / expected, 2)
                if expected else ""
            ),
            "side_output_count": side_counts[key],
        })

    with (OUT / "counts.csv").open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    examples = sorted(
        [
            row for row in rows
            if row["zone"] == "Harbor" and row["expected_count"]
        ],
        key=lambda row: row["side_output_count"],
        reverse=True,
    )[:6]

    print("\nSame window and zone, compared using both clocks:")
    print("Window            Event  Processing  Expected  Complete  Side")

    for row in examples:
        print(
            f"{row['window_start'][:16]}  "
            f"{row['event_time_count']:5}  "
            f"{row['processing_time_count']:10}  "
            f"{row['expected_count']:8}  "
            f"{row['completeness_pct']:7}%  "
            f"{row['side_output_count']:4}"
        )


# ---------------------------------------------------------
# CONSUMER: replay five-minute arrival batches.
# ---------------------------------------------------------
def main():
    require_pyarrow()
    require_deltalake()

    if OUT.exists():
        raise SystemExit(
            "Rename the existing output folder before running again."
        )

    OUT.mkdir()
    events = generate_events()
    write_json_lines(OUT / "events.jsonl", events)

    bands = Counter(event["band"] for event in events)
    assert bands == Counter(fast=4600, medium=350, tail=50)

    print("Generated 5,000 trips:", dict(bands))
    print("Policy: 15-minute window, 12-minute watermark, 5-minute allowance.")

    checkpoint(
        "CHECKPOINT 1: Open output/events.jsonl and show the two timestamps."
    )

    batches = defaultdict(list)

    for event in events:
        arrival = timestamp(event["processing_time"])
        batch_time = arrival.replace(
            minute=arrival.minute // 5 * 5,
            second=0,
            microsecond=0,
        )
        batches[batch_time].append(event)

    accepted = []
    side_output = []

    max_event_time = BASE - timedelta(days=1)
    watermark = max_event_time - timedelta(minutes=WATERMARK_MINUTES)

    evolved = False
    first_side_shown = False
    late_updates = 0

    raw_path = str(OUT / "raw_trips")
    counts_path = str(OUT / "zone_counts")

    for clock, batch in sorted(batches.items()):
        new_column = (
            not evolved
            and any("surge_multiplier" in event for event in batch)
        )

        evolved = evolved or new_column

        write_deltalake(
            raw_path,
            raw_table(batch, evolved),
            mode="append",
            schema_mode="merge",
            configuration=RETENTION,
        )

        for event in batch:
            event_time = timestamp(event["event_time"])

            end = window_start(event_time) + timedelta(
                minutes=WINDOW_MINUTES
            )

            deadline = end + timedelta(minutes=ALLOWED_MINUTES)

            # Compare with the watermark established by earlier arrivals.
            if watermark >= deadline:
                record = dict(
                    event,
                    watermark=watermark.isoformat(),
                    reason="Window acceptance deadline passed",
                )
                side_output.append(record)

            else:
                accepted.append(event)

                if watermark >= end:
                    late_updates += 1
                    print("Accepted late update:", event["trip_id"])

            max_event_time = max(max_event_time, event_time)

            watermark = max_event_time - timedelta(
                minutes=WATERMARK_MINUTES
            )

        write_deltalake(
            counts_path,
            aggregate_table(accepted, watermark, evolved),
            mode="overwrite",
            schema_mode="merge",
            configuration=RETENTION,
        )

        write_json_lines(OUT / "side_output.jsonl", side_output)

        arrived_count = len(accepted) + len(side_output)
        side_pct = 100 * len(side_output) / arrived_count

        freshness_gap = (
            clock + timedelta(minutes=5) - max_event_time
        ).total_seconds() / 60

        if clock.minute in (0, 30) or new_column:
            print(
                f"BATCH {clock.isoformat()} "
                f"accepted={len(accepted)} "
                f"side={len(side_output)} ({side_pct:.2f}%) "
                f"freshness_gap={freshness_gap:.1f} minutes"
            )

        if side_output and not first_side_shown:
            print("\nFirst side-output event:")
            print(json.dumps(side_output[0], indent=2))

            checkpoint(
                "CHECKPOINT 2: This trip missed its deadline. "
                "Its record is preserved for reconciliation."
            )

            first_side_shown = True

        if new_column:
            print("\nDay 4 schema:")
            print(DeltaTable(raw_path).schema().to_arrow())

            checkpoint(
                "CHECKPOINT 3: surge_multiplier now exists. "
                "Earlier records remain available with null values."
            )

    assert len(accepted) + len(side_output) == 5000
    assert len({
        event["trip_id"]
        for event in accepted + side_output
    }) == 5000

    assert side_output
    assert all(event["band"] == "tail" for event in side_output)

    raw_rows = DeltaTable(raw_path).to_pyarrow_table().to_pylist()

    assert any(row["surge_multiplier"] is None for row in raw_rows)
    assert any(row["surge_multiplier"] is not None for row in raw_rows)

    show_comparison(accepted, events, side_output)

    good_delta = DeltaTable(counts_path)
    good_version = good_delta.version()
    good_table = good_delta.to_pyarrow_table()

    checkpoint(
        f"CHECKPOINT 4: Accepted={len(accepted)}, "
        f"side output={len(side_output)}, "
        f"late updates={late_updates}. "
        "Show the clock comparison and output/counts.csv."
    )

    # -----------------------------------------------------
    # BAD CORRECTION: intentionally damage day 2.
    # -----------------------------------------------------
    bad_rows = good_table.to_pylist()

    for row in bad_rows:
        if row["window_start"].startswith("2026-03-02"):
            row["trip_count"] = 0

    write_deltalake(
        counts_path,
        pa.Table.from_pylist(bad_rows, schema=good_table.schema),
        mode="overwrite",
    )

    current = DeltaTable(counts_path).to_pyarrow_table()

    # TIME TRAVEL: read the actual prior Delta version.
    previous = DeltaTable(
        counts_path,
        version=good_version,
    ).to_pyarrow_table()

    print("\nINTENTIONAL BAD CORRECTION: day 2 set to zero.")
    print("Current total:", sum(current["trip_count"].to_pylist()))
    print(
        f"Prior version {good_version} total:",
        sum(previous["trip_count"].to_pylist()),
    )

    assert previous.equals(good_table)

    checkpoint(
        "CHECKPOINT 5: The prior snapshot proves the good counts. "
        "Continue to restore and reconcile."
    )

    # Restore the good version.
    DeltaTable(counts_path).restore(good_version)

    assert DeltaTable(
        counts_path
    ).to_pyarrow_table().equals(good_table)

    # Reconcile from persisted raw data using unique trip IDs.
    unique_trips = {
        event["trip_id"]: event
        for event in raw_rows
    }

    corrected = aggregate_table(
        list(unique_trips.values()),
        watermark,
        evolved=True,
    )

    write_deltalake(
        counts_path,
        corrected,
        mode="overwrite",
        schema_mode="merge",
    )

    corrected_total = sum(corrected["trip_count"].to_pylist())
    assert corrected_total == 5000

    # Repeating the calculation produces the same counts.
    repeated = aggregate_table(
        list(unique_trips.values()),
        watermark,
        evolved=True,
    )
    assert repeated.equals(corrected)

    summary = {
        "generated": 5000,
        "accepted": len(accepted),
        "side_output": len(side_output),
        "side_output_pct": 100 * len(side_output) / 5000,
        "late_updates": late_updates,
        "good_version": good_version,
        "reconciled_total": corrected_total,
        "checks": "PASS",
    }

    (OUT / "summary.json").write_text(
        json.dumps(summary, indent=2)
    )

    print("\nRestored the good version and reconciled raw trips.")
    print(json.dumps(summary, indent=2))
    print("Total=5000. CHECKS PASS.")
    print("No vacuum performed during this demonstration.")


if __name__ == "__main__":
    main()