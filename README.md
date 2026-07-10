# Ikigai — Engraving Station Cart-Scan Logger

**Your own system. Not connected to ShipHero.** Each engraving station's tablet opens a scan
page; the engraver scans **each tote when they finish engraving it**; the scan is written to your
own spreadsheet as `scanned_at, station, tote, engraver`.

## Why this exists
ShipHero attributes a pick to **one person per order** — so engraving credit can't live in the
pick record without a last-touch person stealing it. This logger sits entirely outside ShipHero.
Credit follows the **station** that scanned the completed tote, joined afterward to:
- **WorkforceHero** — who was badged into that station at that time (and their hours), and
- the tote's **engraving items** — the output (which/how many, by type).

## How the engraver uses it
1. Open the tablet at the station → it shows a big **Engraving 1 / 2 / 3** page (pick once per device).
2. (Optional) scan/enter your badge once at shift start.
3. **Finish engraving a tote → scan it.** A green ✓ confirms, a running count shows the session total.
Re-scanning the same tote within ~45s is flagged (not double-counted).

## Resolving scans to credit — pick the identifier
The logger records **whatever the scanner reads**, so the app doesn't change either way. What
matters is *which barcode* the engraver scans, because that's how we turn a scan into engraving units:

| They scan… | How we resolve it to engraving items | Extra dependency |
|---|---|---|
| the **order / packing-slip barcode** | directly, order → engraving lines (already proven) | **none** — recommended |
| the **ShipHero tote barcode** | via the **Tote Complete webhook** (tote barcode → orders → items) | the webhook listener + ShipHero support flag |

**Recommendation:** if the case/tote carries a scannable order label, scan that — it resolves with
zero extra ShipHero setup. If only a ShipHero tote barcode is available, we join on it using the
Tote Complete webhook listener (already built in `../shiphero-engraving-webhook`).

> Verify first: send me one real scanned value and I'll confirm it resolves to the order + engraving
> items before we finalize the join.

## Files
| File | Purpose |
|---|---|
| `app.py` | the Flask app: station picker, per-station scan page, `/log` endpoint |
| `sinks.py` | CSV + Google-Sheet sinks and the column schema |
| `requirements.txt`, `.env.example`, `render.yaml` | deps, config, Render blueprint |

## Run locally
```bash
pip install -r requirements.txt
python app.py                       # http://localhost:8080
# open http://localhost:8080/scan?station=Engraving%201 and scan (or type + Enter)
cat cart_scans.csv
```

## Deploy on Render (you already use Render)
1. Put this folder in a repo; Render → **New + → Blueprint** at it (reads `render.yaml`).
2. Add the Google service-account key as a **Secret File** named `service_account.json`, and share
   the destination Sheet with the service-account email (Editor).
3. Set `GSHEET_ID` in the dashboard. Deploy → you get a URL like
   `https://ikigai-engraving-station-logger.onrender.com`.
4. On each station tablet, open `…/scan?station=Engraving%201` (or 2 / 3) and leave it up. Tip: add
   it to the tablet home screen so it reopens full-screen.

## Notes
- Works with any USB/Bluetooth barcode scanner (they act as a keyboard: type + Enter).
- The page auto-refocuses the input so a scan always lands, and polls `/recent` so a supervisor
  can watch it.
- Runs `workers=1` so the recent-scans view and dedup share one process; fine for 3 stations.
