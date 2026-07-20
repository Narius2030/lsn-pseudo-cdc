# LSN Windowing & Adaptive Extraction

## LSN (Log Sequence Number) Concepts

An LSN is a **10-byte binary value** that SQL Server uses to order changes in the transaction log. Each change in a CDC change table has:

| Column | Meaning |
|---|---|
| `__$start_lsn` | LSN of the committed transaction (the same value for every row in that transaction) |
| `__$seqval` | Local LSN within the transaction, distinguishing different changes |
| `__$command_id` | Statement execution order within the transaction (available only from CU10+) |
| `__$operation` | Change type: 1=delete, 2=insert, 3=update (before), 4=update (after) |

## LSN Format Conversions

```
Raw bytes (10 bytes):     0x00000028000001A40003
Hex string:               0x00000028000001A40003
Debezium format:          00000028:000001A4:0003
                          ────────  ────────  ────
                          VLF seq   Log block  Slot
```

## max_lsn — The Run Ceiling

At Stage 4, the pipeline calls `sys.fn_cdc_get_max_lsn()` and **fixes this value for the entire run**. It is the ceiling for every table in the run—no table reads events newer than this point.

### Why is `max_lsn` needed?

1. **Bounding query scope**: Every query, `WHERE __$start_lsn >= @from AND __$start_lsn <= @max_lsn`, has a clear upper bound. Without a ceiling, a query can return rows being written (an unbounded result set and phantom reads).

2. **Advancing bookmarks uniformly**: After processing completes, every table sets its bookmark to `max_lsn`, including tables with no changes. Otherwise, a table with no changes never advances its bookmark and re-scans an empty range on every run.

3. **Deterministic retries**: If a run fails partway through and is retried, the same `max_lsn` produces the same window boundaries, making the retry idempotent.

### Example

```
Run starts at T0 → max_lsn = 0x0000002A (frozen)
│
├─ Table A: bookmark=0x20 → read 0x21..0x2A → 5 events → bookmark=0x2A
├─ Table B: bookmark=0x25 → read 0x26..0x2A → 2 events → bookmark=0x2A  
├─ Table C: bookmark=0x2A → read 0x2B..0x2A → EMPTY (from > to) → bookmark=0x2A
│
│  (transactions 0x2B and 0x2C arrive DURING the run → IGNORED)
│
└─ Save all bookmarks = 0x2A

Next run → max_lsn = 0x0000002F
├─ All tables start from increment(0x2A) = 0x2B
└─ Picks up 0x2B..0x2F — no gaps or duplicates
```

**In short**: `max_lsn` is a snapshot of the run start time. It acts as the maximum stopping point through which each table can consume events into JSON. Any event that arrives afterward is processed in the next run.

---

## Adaptive Window Algorithm

Rather than reading all CDC changes at once, the pipeline divides them into **windows**—each containing at most N distinct LSN values.

### Flow

```
                    ┌───────────────────────────┐
                    │ current_from_lsn = start  │
                    │ window_size = configured  │
                    └───────────┬───────────────┘
                                │
                    ┌───────────▼───────────────┐
             ┌─────│ compare(from_lsn, max_lsn)│
             │     └───────────┬───────────────┘
             │                 │ from_lsn <= max_lsn
             │     ┌───────────▼───────────────┐
             │     │ get_window_end_lsn()      │
             │     │ (SELECT DISTINCT TOP N    │
             │     │  __$start_lsn ... MAX())  │
             │     └───────────┬───────────────┘
             │                 │
             │     ┌───────────▼───────────────┐
             │     │ Try: extract_single_window│
             │     └───────────┬───────────────┘
             │                 │
             │         ┌───────┴───────┐
             │         │               │
             │     SUCCESS          SQL ERROR
             │         │               │
             │         ▼               ▼
             │   ┌───────────┐  ┌──────────────────────┐
             │   │ Upload    │  │ Shrinkable error?    │
             │   │ temp file │  │ (timeout/memory/     │
             │   │ to S3     │  │  deadlock)           │
             │   └─────┬─────┘  └──────┬───────────────┘
             │         │               │ YES
             │         │       ┌───────▼───────────────┐
             │         │       │ window_size /= factor │
             │         │       │ (min: min_window_size)│
             │         │       └───────┬───────────────┘
             │         │               │
             │         │       ┌───────▼───────────────┐
             │         │       │ Retry same from_lsn   │
             │         │       │ with smaller window   │
             │         │       └───────────────────────┘
             │         │
             │         ▼
             │   ┌───────────────────────┐
             │   │ from_lsn = increment( │
             │   │   current_to_lsn)     │
             │   └───────────┬───────────┘
             │               │
             └───────────────┘ (loop)
```

### Window End LSN Query

```sql
SELECT MAX(window_lsn) AS window_end_lsn
FROM (
    SELECT DISTINCT TOP (@window_size) __$start_lsn AS window_lsn
    FROM cdc.[capture_instance_CT]
    WHERE __$start_lsn >= @from_lsn AND __$start_lsn <= @max_lsn
    ORDER BY __$start_lsn
) AS windowed;
```

**Explanation**: Take `window_size` distinct `__$start_lsn` values (that is, N transactions), then take their maximum. The result is the window boundary. Each transaction can contain multiple rows.

### Window Shrinking

When a timeout, memory, or deadlock error occurs:

```
initial_window_size = 1000 (configured)
                         │
                    timeout error
                         │
                         ▼
              window_size = 1000 / 2 = 500
                         │
                    timeout error
                         │
                         ▼
              window_size = 500 / 2 = 250
                         │
                    timeout error
                         │
                         ▼
              window_size = 250 / 2 = 125
                         │
               (125 > min_lsn_window_size=100)
                         │
                    timeout error
                         │
                         ▼
              window_size = max(100, 125/2) = 100 ← FLOOR
                         │
                    timeout error
                         │
                         ▼
              RAISE (cannot shrink further)
```

**Parameters**:
- `lsn_window_size`: Initial window size (default: 5000)
- `min_lsn_window_size`: Lower bound; the window cannot shrink below it (default: 100)
- `window_size_reduction_factor`: Divisor used when shrinking (default: 2)

### LSN Increment

After each successful window, the pipeline calls:

```sql
SELECT sys.fn_cdc_increment_lsn(@current_to_lsn) AS next_lsn
```

This built-in function returns the next LSN, ensuring no changes are skipped or read twice.

## Change Group Ordering

Each window is read with an `ORDER BY` clause that guarantees the correct order:

```sql
SELECT ...
FROM cdc.[capture_instance_CT] AS ct
WHERE ct.__$start_lsn >= @from AND ct.__$start_lsn <= @to
ORDER BY ct.__$start_lsn, ct.__$command_id, ct.__$seqval, ct.__$operation;
```

**Grouping key**: `(__$start_lsn, __$command_id, __$seqval)`—all rows with the same group key belong to one logical change (for example, an update pair has two rows: op=3 before and op=4 after).

## Transform Logic (Operation Codes)

### CDC Operation Codes

| Code | Meaning |
|---|---|
| 1 | Delete |
| 2 | Insert |
| 3 | Update — row image **before** the change |
| 4 | Update — row image **after** the change |

### Group → Event Mapping

```python
# Group rows sorted by operation code
operations = sorted(rows, key=op)

if operations == [3, 4]:
    # Normal update pair
    → emit 1 event: op="u", before=row[op=3], after=row[op=4]

elif operations == [1, 2]:
    # Delete + Insert in same seqval → primary key change
    if primary_key_changed(row[op=1], row[op=2]):
        → emit: op="d" (delete old PK) + op="c" (create new PK)
    else:
        # Same PK: treat as update
        → emit 1 event: op="u", before=row[op=1], after=row[op=2]

elif operations == [1]:
    → emit: op="d"

elif operations == [2]:
    → emit: op="c"

else:
    # Incomplete pair (3 without 4, etc.)
    if fail_on_incomplete_update_pair:
        raise TransformError
    else:
        # Fallback: emit individual events (skip op=3/4 orphans)
```

### Primary Key Change Detection

When CDC records a primary-key change, SQL Server emits an `(op=1, op=2)` pair with the same `__$seqval`. The pipeline detects it by comparing PK columns between the before and after images:

```python
def _primary_key_changed(before, after, pk_columns):
    return any(before[col] != after[col] for col in pk_columns)
```

If the PK changed, it emits a DELETE for the old record and a CREATE for the new one, matching Debezium semantics.

## SQL Server 2014 SP1 & `__$command_id`

### Problem

Before **CU10** (build 12.0.4491.0, released 2016-12-19), CDC change tables **did not have** the `__$command_id` column. This causes the following issues:

- Within one transaction, multiple UPDATE statements on the same table cannot be ordered distinctly.
- The ordering between before and after images can be incorrect.

### Pipeline solution

```
┌──────────────────────────────┐
│ Build >= CU10 (12.0.4491)?   │
├──────────┬───────────────────┤
│   YES    │       NO          │
│          │                   │
│ ORDER BY │  allow_best_      │
│ includes │  effort_without_  │
│ command_id  command_id=true? │
│          │        │          │
│  SAFE    │   YES: warning    │
│  MODE    │   NO: raise error │
└──────────┴───────────────────┘
```

**Mode labels**:
- `change_table_command_id`: All capture instances have `__$command_id` → strict ordering.
- `change_table_best_effort`: At least one instance lacks it → ordering is not guaranteed.

## Bookmark (State) Management

### Format

```json
{
  "capture_instances": {
    "dbo_MdCustomer": "0x00000028000001A40003",
    "dbo_MdProduct": "0x0000002A000002B50001"
  }
}
```

Each capture instance stores the `max_lsn` of its last successful run.

### Resume Logic

```
                 ┌────────────────────┐
                 │ bookmark exists?   │
                 └──────┬─────────────┘
                        │
              ┌─────────┴─────────┐
              │ YES               │ NO
              │                   │
              ▼                   ▼
    ┌──────────────────┐  ┌─────────────────────┐
    │ saved_lsn >=     │  │ start from          │
    │ current_min_lsn? │  │ capture_min_lsn     │
    └────┬─────────────┘  │ + optional snapshot │
         │                 └─────────────────────┘
    ┌────┴────┐
    │YES      │NO (gap!)
    │         │
    ▼         ▼
 increment   fail_on_lsn_gap?
 (saved+1)   ├─ true: raise CDCGapError
             └─ false: reset to min_lsn (warning)
```

### Gap Scenario

CDC has a retention period (three days by default). If the pipeline does not run for longer than the retention period, its old bookmark is cleaned up, creating an **LSN gap**.

## Retry Strategy

### Levels of Retry

1. **Connection level**: Reconnect on a transport error (`08S01`, `08001`).
2. **Window level**: Shrink the window on a timeout, memory, or deadlock error.
3. **Operation level**: Use exponential backoff for each SQL/S3 operation.

### Exponential Backoff

```
delay = min(max_delay, base_delay × 2^(attempt-1)) + jitter(10%)
```

Default: `base=2s`, `max=30s`, `attempts=3`

```
Attempt 1: 2.0s + jitter
Attempt 2: 4.0s + jitter  
Attempt 3: 8.0s + jitter → FAIL (max_retries exceeded)
```

### Retryable SQL Errors

| SQLSTATE | Error |
|---|---|
| `HYT00`, `HYT01` | Timeout |
| `08S01`, `08001` | Communication link failure |
| `40001` | Deadlock |

Keywords in message: `timeout`, `deadlock`, `communication link failure`, `transport-level error`, `connection is busy`
