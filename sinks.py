"""Sinks for the engraving-station cart-scan logger.

  SINK=csv    -> append to a local CSV (zero setup; good for testing)
  SINK=gsheet -> append to a Google Sheet via a service account (gspread)

Both expose: sink.append_rows(list_of_dicts).
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

def make_sink():
    kind = os.environ.get("SINK", "csv").lower()
    if kind == "webapp":
        return WebAppSink(os.environ["GSHEET_WEBAPP_URL"])
    if kind == "gsheet":
        return GSheetSink(os.environ["GSHEET_ID"], os.environ.get("GSHEET_WORKSHEET", "Cart Scans"))
    return CsvSink(os.environ.get("CSV_PATH", "./cart_scans.csv"))
