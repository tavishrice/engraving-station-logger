"""Background writer for the FULFILLMENT flow.

Writes each fulfilled Shopify (rush) order straight into the contribution
Postgres `event` table as a pack credit for the real packer:

    stage='pack', source='logger', action='fulfill', quantity=NULL

`quantity` is left NULL on purpose -- a small resolver on the contribution side
(fulfill_resolve.py, mirrors engrave_resolve.py) fills the item count per order
by looking the order up. Order count works immediately; item count appears once
the resolver runs.

WHY A BACKGROUND THREAD (this is the whole point):
The July outage was caused by a *blocking* Postgres connect in the scan request
path. Here the request path NEVER touches the DB -- a scan only drops a row on an
in-memory queue and returns instantly. A single daemon worker drains the queue
and writes with a fail-fast connect (see db.connect / PG_CONNECT_TIMEOUT). If the
DB is slow or down, scans keep working; rows wait in the queue and flush when the
DB comes back. Nothing at the station can ever hang on the database.

dedup_key = "logger|fulfill|<ORDER>" -- one pack credit per order (idempotent to
re-scans; first scanner wins if two people scan the same order).
"""
import os, json, queue, threading, time
from datetime import datetime, timezone

try:
    from db import connect          # shared fail-fast Postgres helper
except Exception as _e:             # pragma: no cover - keeps app bootable w/o db.py
    connect = None
    print("[fulfill] WARNING: db.connect unavailable:", repr(_e), flush=True)

_MAXQ = int(os.environ.get("FULFILL_QUEUE_MAX", "10000"))
_q = queue.Queue(maxsize=_MAXQ)
_lock = threading.Lock()
_stats = {"enqueued": 0, "written": 0, "conflict": 0, "dropped": 0,
          "last_error": None, "last_write": None}

INSERT = (
    "INSERT INTO event (ts, person, stage, station, action, order_number, source, raw, dedup_key) "
    "VALUES (%(ts)s, %(person)s, 'pack', %(station)s, 'fulfill', %(order)s, 'logger', %(raw)s::jsonb, %(dedup)s) "
    "ON CONFLICT (dedup_key) DO NOTHING"
)


def norm_order(s):
    """Normalize a scanned order value: trim, drop a leading '#', upper-case."""
    return (s or "").strip().lstrip("#").strip().upper()


def record(person, station, order, scanned_raw=None, ts=None):
    """Enqueue one fulfilled order for a background DB write. Never blocks on I/O."""
    _ensure_worker()
    person = (person or "").strip()
    ordn = norm_order(order)
    if not person or not ordn:
        return False
    ts = ts or datetime.now(timezone.utc)
    raw = json.dumps({
        "flow": "fulfillment",
        "station": station,
        "by": person,
        "scanned": order if scanned_raw is None else scanned_raw,
    })
    item = {"ts": ts, "person": person, "station": station, "order": ordn,
            "raw": raw, "dedup": "logger|fulfill|%s" % ordn}
    try:
        _q.put_nowait(item)
        with _lock:
            _stats["enqueued"] += 1
        return True
    except queue.Full:
        with _lock:
            _stats["dropped"] += 1
            _stats["last_error"] = "queue full"
        print("[fulfill] queue full -- dropped", ordn, flush=True)
        return False


def _write(batch):
    if connect is None:
        with _lock:
            _stats["last_error"] = "db module unavailable"
        return False
    try:
        with connect() as c, c.cursor() as cur:
            w = cf = dl = 0
            for it in batch:
                if it.get("_op") == "delete":
                    cur.execute(DELETE, it)
                    dl += cur.rowcount
                    continue
                cur.execute(INSERT, it)
                if cur.rowcount:
                    w += 1
                else:
                    cf += 1
            c.commit()
        with _lock:
            _stats["written"] += w
            _stats["conflict"] += cf
            _stats["voided"] = _stats.get("voided", 0) + dl
            _stats["last_write"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            _stats["last_error"] = None
        print("[fulfill] +%d event(s), %d dup, -%d void" % (w, cf, dl), flush=True)
        return True
    except Exception as e:
        with _lock:
            _stats["last_error"] = repr(e)
        print("[fulfill] write error:", repr(e), flush=True)
        return False


def _drain(timeout=1.0, maxn=50):
    batch = []
    try:
        batch.append(_q.get(timeout=timeout))
    except queue.Empty:
        return batch
    while len(batch) < maxn:
        try:
            batch.append(_q.get_nowait())
        except queue.Empty:
            break
    return batch


def _worker():
    while True:
        batch = _drain()
        if not batch:
            continue
        ok = _write(batch)
        if not ok:                      # fail-fast connect already bounded the wait
            for attempt in range(3):    # brief retries so a blip doesn't lose data
                time.sleep(min(2 * (attempt + 1), 10))
                if _write(batch):
                    ok = True
                    break
        if not ok:
            with _lock:
                _stats["dropped"] += len(batch)
            print("[fulfill] dropped %d row(s) after retries" % len(batch), flush=True)
        for _ in batch:
            _q.task_done()


DELETE = "DELETE FROM event WHERE dedup_key = %(dedup)s AND source = 'logger'"


def void(order):
    """Undo a fulfillment scan: enqueue a DELETE of that order's logger pack row.

    FIFO with record(): if the INSERT is still queued it runs first, then this
    DELETE removes it; if already written, this DELETE removes it. Idempotent.
    """
    _ensure_worker()
    ordn = norm_order(order)
    if not ordn:
        return False
    item = {"_op": "delete", "dedup": "logger|fulfill|%s" % ordn, "order": ordn}
    try:
        _q.put_nowait(item)
        return True
    except queue.Full:
        print("[fulfill] queue full -- undo dropped", ordn, flush=True)
        return False


def stats():
    with _lock:
        s = dict(_stats)
    s["queued"] = _q.qsize()
    return s


_worker_thread = None
_start_lock = threading.Lock()


def _ensure_worker():
    """Guarantee a LIVE writer thread in THIS process.

    Called from record()/void() (which run in the gunicorn worker process, after
    any fork). A thread started at import under `gunicorn --preload` lives only in
    the master and does NOT survive the fork, so an import-time start is not
    enough -- we (re)start here if the thread isn't alive in this process.
    """
    global _worker_thread
    if _worker_thread is not None and _worker_thread.is_alive():
        return
    with _start_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return
        _worker_thread = threading.Thread(target=_worker, name="fulfill-writer", daemon=True)
        _worker_thread.start()
        print("[fulfill] background writer started (pid-local)", flush=True)


def start():
    """Kept for import-time call in app.py; real guarantee is _ensure_worker()."""
    _ensure_worker()
