"""Sinks for the engraving-station cart-scan logger.

  SINK=csv        -> append to a local CSV (zero setup; good for testing)
  SINK=gsheet     -> append to a Google Sheet via a service account (gspread)
  SINK=webapp     -> POST each row to a Google Apps Script Web App (the Sheet)
  SINK=pg         -> insert each event into Postgres (the contribution store)
  SINK=pg+webapp  -> DUAL WRITE: Postgres AND the Sheet (safe during transition)

All expose: sink.append_rows(list_of_dicts).

--- Live Postgres path (2026-07-15) -------------------------------------------------
PgSink writes SYNCHRONOUSLY but SAFELY. The earlier outage was caused by a synchronous
psycopg.connect() with NO timeout: when the DB was momentarily unreachable the single
gunicorn worker hung forever and the scan station went down. Two things prevent that now:
  1. db.connect() uses connect_timeout (see db.py) so a bad DB fails fast (<= ~8s),
     never an infinite hang.
  2. append_rows() NEVER raises — any DB error is caught and logged, and because the
     logger runs SINK=pg+webapp, the Google Sheet dual-write always still has the row.
So the worst case when the DB is unreachable is a few seconds of extra latency per scan
(then it recovers), not an outage.

(An earlier attempt wrote to PG from a background daemon thread to keep the request
path instant. That works locally and in a direct container shell, but the daemon
thread does not get scheduled inside Render's single sync gunicorn worker, so rows
never flushed over HTTP. Synchronous + connect_timeout is simpler and reliable.)
"""
import csv, os, threading

COLUMNS = [
    "ts",           # UTC time of the event
    "station",      # Engraving 1 / 2 / 3
    "event",        # login | logout | scan
    "engraver",     # badge/name logged in at the station
    "tote",         # scanned tote/order value (blank for login/logout)
    "source",       # "station-tablet"
    "note",
]

class CsvSink:
    def __init__(self, path):
        self.path = path; self.lock = threading.Lock()
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=COLUMNS).writeheader()
    def append_rows(self, rows):
        with self.lock, open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            for r in rows: w.writerow({c: r.get(c, "") for c in COLUMNS})
        print(f"[csv] +{len(rows)} -> {self.path}", flush=True)

class GSheetSink:
    def __init__(self, sheet_id, worksheet):
        import gspread
        self.lock = threading.Lock()
        self.gc = gspread.service_account(filename=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        sh = self.gc.open_by_key(sheet_id)
        try: self.ws = sh.worksheet(worksheet)
        except Exception: self.ws = sh.add_worksheet(title=worksheet, rows=2000, cols=len(COLUMNS))
        if self.ws.row_values(1) != COLUMNS: self.ws.update("A1", [COLUMNS])
    def append_rows(self, rows):
        values = [[str(r.get(c, "")) for c in COLUMNS] for r in rows]
        with self.lock: self.ws.append_rows(values, value_input_option="RAW")
        print(f"[gsheet] +{len(rows)}", flush=True)

class WebAppSink:
    """Posts each row to a Google Apps Script Web App URL, which appends it to the
    bound Google Sheet. No service-account key needed — only the /exec URL (config)."""
    def __init__(self, url):
        import urllib.request, json as _json
        self._url = url; self._req = urllib.request; self._json = _json
        self.lock = threading.Lock()
    def append_rows(self, rows):
        ok = 0
        for r in rows:
            body = self._json.dumps({c: r.get(c, "") for c in COLUMNS}).encode()
            req = self._req.Request(self._url, data=body,
                                    headers={"Content-Type": "application/json"})
            with self.lock:
                try:
                    self._req.urlopen(req, timeout=10).read(); ok += 1
                except Exception as e:
                    print("[webapp] error:", e, flush=True)
        print(f"[webapp] posted {ok}/{len(rows)} row(s)", flush=True)

class PgSink:
    """Insert each logger event into the Postgres `event` table (stage='engrave').
    Same store the Tote Complete webhook writes to, so scans + tote contents can be
    joined for engraving credit (the join lives in refresh_contribution_day()).

    Synchronous but SAFE: bounded by db.connect()'s connect_timeout and never raises,
    so a DB problem costs a little latency, never the scan station (see module docstring)."""
    ALIAS = {"User-777001": "Maurice Williams"}   # canonicalize known aliases
    INSERT = ("INSERT INTO event (ts,person,stage,station,action,tote_barcode,source,dedup_key) "
              "VALUES (%s,%s,'engrave',%s,%s,%s,'logger',%s) ON CONFLICT (dedup_key) DO NOTHING")

    def __init__(self):
        from db import connect
        self._connect = connect
        self.lock = threading.Lock()

    def _rowvals(self, r):
        person = self.ALIAS.get(r.get("engraver"), r.get("engraver"))
        tote = r.get("tote")
        tote = str(tote) if tote not in (None, "") else None
        ts, action = r.get("ts"), r.get("event")
        dedup = "|".join(["logger", str(ts), str(person), action or "", tote or ""])
        return (ts, person, r.get("station"), action, tote, dedup)

    def append_rows(self, rows):
        vals = [self._rowvals(r) for r in rows]
        if not vals:
            return 0
        try:
            with self.lock, self._connect() as c, c.cursor() as cur:
                cur.executemany(self.INSERT, vals)
                c.commit()
            print(f"[pg] +{len(vals)} event(s)", flush=True)
            return len(vals)
        except Exception as e:
            # NEVER propagate into the request path — the Google Sheet dual-write keeps the row.
            print(f"[pg] write failed (row kept in Sheet): {e!r}", flush=True)
            return 0

class MultiSink:
    """Fan out to several sinks; one sink failing never blocks the others."""
    def __init__(self, sinks): self.sinks = sinks
    def append_rows(self, rows):
        for s in self.sinks:
            try: s.append_rows(rows)
            except Exception as e:
                print(f"[multi] {type(s).__name__} error: {e!r}", flush=True)

def _one(kind):
    kind = kind.strip().lower()
    if kind == "webapp": return WebAppSink(os.environ["GSHEET_WEBAPP_URL"])
    if kind == "gsheet": return GSheetSink(os.environ["GSHEET_ID"], os.environ.get("GSHEET_WORKSHEET", "Cart Scans"))
    if kind == "pg":     return PgSink()
    return CsvSink(os.environ.get("CSV_PATH", "./cart_scans.csv"))

def make_sink():
    kind = os.environ.get("SINK", "csv").lower()
    if "+" in kind:                       # e.g. "pg+webapp" -> dual write
        return MultiSink([_one(k) for k in kind.split("+")])
    return _one(kind)
