"""
Engraving Station Logger — Ikigai Cases.  (login/logout + cart scans)

A standalone system (NOT connected to ShipHero). Each engraving station opens a page on its
tablet. The engraver LOGS IN (scans/enters their badge), scans each tote as they finish it, and
LOGS OUT at the end. Everything is written to your own spreadsheet as one event log:

    ts, station, event, engraver, tote        (event = login | logout | scan)

Why: ShipHero attributes a pick to ONE person per order, so engraving credit can't live there.
This is decoupled from ShipHero. Because it also captures login/logout, it gives you engraver
HOURS automatically (login->logout per station) — no manual WorkforceHero export needed; WFH just
becomes a cross-check. And every cart scan is stamped with the engraver who is logged in.

Endpoints:
  GET  /                          station picker
  GET  /scan?station=Engraving%201  the station page
  GET  /state?station=...         current logged-in engraver + recent scans (page polls this)
  POST /login   {station, engraver}
  POST /logout  {station}
  POST /log     {station, tote}   (requires someone logged in at that station)
  GET  /health

Env (see .env.example): STATIONS, SINK, CSV_PATH / GSHEET_*, DEDUP_SECONDS, PORT
"""
import os, json, queue, threading
from collections import deque, defaultdict
from datetime import datetime, timezone

from flask import Flask, request, jsonify, render_template_string, redirect
from sinks import make_sink, COLUMNS

app = Flask(__name__)
STATIONS = [s.strip() for s in os.environ.get(
    "STATIONS", "Engraving 1,Engraving 2,Engraving 3").split(",") if s.strip()]
DEDUP_SECONDS = int(os.environ.get("DEDUP_SECONDS", "45"))

_sink = make_sink()
_q: "queue.Queue" = queue.Queue()
_current = {}                                    # station -> engraver currently logged in
_recent = defaultdict(lambda: deque(maxlen=25))  # station -> recent scan cards
_count = defaultdict(int)                         # station -> scans since login
_last = {}                                        # (station,tote) -> iso ts (dedup)
_lock = threading.Lock()

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _worker():
    while True:
        row = _q.get()
        try: _sink.append_rows([row])
        except Exception as e: print("[sink] error:", e, flush=True)
        finally: _q.task_done()
threading.Thread(target=_worker, daemon=True).start()

def emit(station, event, engraver="", tote="", note=""):
    _q.put({"ts": now_iso(), "station": station, "event": event,
            "engraver": engraver, "tote": tote, "source": "station-tablet", "note": note})

PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{station}}</title>
<style>
 :root{color-scheme:dark} *{box-sizing:border-box}
 body{margin:0;font-family:system-ui,Arial,sans-serif;background:#0f1720;color:#e7eef7}
 header{background:#1F3B57;padding:16px 22px;display:flex;justify-content:space-between;align-items:center}
 header h1{margin:0;font-size:26px}
 main{padding:22px;max-width:760px;margin:0 auto}
 .who{font-size:19px} .who b{color:#7CE0A0}
 form{display:flex;gap:12px;margin:10px 0 16px}
 input[type=text]{flex:1;font-size:30px;padding:16px 18px;border-radius:12px;border:2px solid #2E75B6;background:#0b121a;color:#fff}
 button{font-size:19px;padding:0 22px;border:0;border-radius:12px;background:#2E75B6;color:#fff;cursor:pointer}
 button.out{background:#5b3a44}
 .flash{font-size:22px;padding:12px 16px;border-radius:12px;margin:10px 0;min-height:24px}
 .ok{background:#123f24;color:#7CE0A0} .dup{background:#4a3a10;color:#ffd873} .err{background:#4a1620;color:#ff9aa8}
 table{width:100%;border-collapse:collapse;font-size:18px} td,th{padding:9px 8px;border-bottom:1px solid #22303f;text-align:left}
 .muted{opacity:.6;font-size:15px} a{color:#8fc1ee} .hide{display:none}
</style></head><body>
<header><h1>{{station}}</h1><div class="who" id="who"></div></header>
<main>
  <div class="muted"><a href="/">switch station</a></div>
  <div id="flash" class="flash"></div>

  <div id="loginBox">
    <form id="loginForm"><input id="badge" type="text" placeholder="Scan your badge to log in…" autofocus>
      <button type="submit">Log in</button></form>
  </div>

  <div id="scanBox" class="hide">
    <form id="scanForm"><input id="tote" type="text" placeholder="Scan finished tote…">
      <button type="submit">Log tote</button></form>
    <div style="margin:6px 0 16px"><button class="out" id="logout" type="button">Log out</button>
      <span class="muted" style="margin-left:10px"><span id="cnt">0</span> totes this session</span></div>
    <table><thead><tr><th>Time</th><th>Tote</th></tr></thead><tbody id="rows">
      <tr><td colspan="2" class="muted">no totes yet</td></tr></tbody></table>
  </div>
</main>
<script>
const station = {{station_json}};
const $=id=>document.getElementById(id);
const flash=$('flash');
function setFlash(cls,msg){flash.className='flash '+cls;flash.textContent=msg;}
function show(engraver, recent, count){
  if(engraver){
    $('who').innerHTML='▣ <b>'+engraver+'</b> logged in';
    $('loginBox').classList.add('hide'); $('scanBox').classList.remove('hide');
    $('cnt').textContent=count||0; render(recent||[]); $('tote').focus();
  } else {
    $('who').textContent='not logged in';
    $('scanBox').classList.add('hide'); $('loginBox').classList.remove('hide'); $('badge').focus();
  }
}
function render(list){ if(list.length) $('rows').innerHTML=list.map(x=>`<tr><td>${x.t}</td><td>${x.tote}</td></tr>`).join(''); }
async function api(path,body){ const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); return r.json(); }

$('loginForm').addEventListener('submit',async e=>{e.preventDefault();
  const b=$('badge').value.trim(); if(!b)return; $('badge').value='';
  const j=await api('/login',{station,engraver:b});
  setFlash('ok','✓ '+j.engraver+' logged in'); show(j.engraver,[],0);
});
$('scanForm').addEventListener('submit',async e=>{e.preventDefault();
  const t=$('tote').value.trim(); if(!t){$('tote').focus();return;} $('tote').value='';
  const j=await api('/log',{station,tote:t});
  if(j.status==='ok'){setFlash('ok','✓ '+t+' logged');}
  else if(j.status==='duplicate'){setFlash('dup','⟳ '+t+' — just scanned');}
  else if(j.status==='not_logged_in'){setFlash('err','✕ log in first');show(null);return;}
  else {setFlash('err','✕ '+(j.message||'error'));}
  if(j.recent){render(j.recent);} if(j.count!=null){$('cnt').textContent=j.count;} $('tote').focus();
});
$('logout').addEventListener('click',async()=>{ const j=await api('/logout',{station}); setFlash('ok','logged out'); show(null); });

document.addEventListener('click',()=>{ (document.getElementById('scanBox').classList.contains('hide')?$('badge'):$('tote')).focus(); });
async function poll(){ try{const r=await fetch('/state?station='+encodeURIComponent(station));const j=await r.json();
  if(!j.engraver){show(null);} else { $('cnt').textContent=j.count; render(j.recent||[]); } }catch(e){} }
poll(); setInterval(poll, 20000);
</script></body></html>
"""

PICKER = """<!doctype html><meta charset=utf-8><title>Engraving Stations</title>
<style>body{font-family:system-ui,Arial;background:#0f1720;color:#e7eef7;text-align:center;padding:40px}
a{display:block;font-size:30px;background:#2E75B6;color:#fff;text-decoration:none;padding:22px;border-radius:14px;margin:14px auto;max-width:420px}</style>
<h1>Pick this tablet's station</h1>{{ links|safe }}"""

@app.get("/")
def picker():
    links = "".join(f'<a href="/scan?station={s.replace(" ","%20")}">{s}</a>' for s in STATIONS)
    return render_template_string(PICKER, links=links)

@app.get("/scan")
def scan_page():
    station = request.args.get("station", "")
    if station not in STATIONS: return redirect("/")
    return render_template_string(PAGE, station=station, station_json=json.dumps(station))

@app.get("/health")
def health():
    return jsonify(status="ok", stations=STATIONS,
                   logged_in={s: _current.get(s) for s in STATIONS}, queue=_q.qsize())

@app.get("/state")
def state():
    station = request.args.get("station", "")
    with _lock:
        return jsonify(engraver=_current.get(station), count=_count.get(station, 0),
                       recent=list(_recent.get(station, [])))

@app.post("/login")
def login():
    d = request.get_json(silent=True) or {}
    station, engraver = (d.get("station") or "").strip(), (d.get("engraver") or "").strip()
    if station not in STATIONS or not engraver:
        return jsonify(status="error", message="missing station or badge"), 400
    with _lock:
        _current[station] = engraver
        _count[station] = 0
        _recent[station].clear()
    emit(station, "login", engraver=engraver)
    return jsonify(status="ok", engraver=engraver)

@app.post("/logout")
def logout():
    d = request.get_json(silent=True) or {}
    station = (d.get("station") or "").strip()
    with _lock:
        eng = _current.pop(station, "")
    emit(station, "logout", engraver=eng)
    return jsonify(status="ok")

@app.post("/log")
def log_scan():
    d = request.get_json(silent=True) or {}
    station, tote = (d.get("station") or "").strip(), (d.get("tote") or "").strip()
    if station not in STATIONS or not tote:
        return jsonify(status="error", message="missing station or tote"), 400
    with _lock:
        engraver = _current.get(station)
        if not engraver:
            return jsonify(status="not_logged_in"), 200
        ts = now_iso()
        key = (station, tote)
        prev = _last.get(key)
        dup = prev and (datetime.fromisoformat(ts) - datetime.fromisoformat(prev)).total_seconds() < DEDUP_SECONDS
        _last[key] = ts
        if not dup:
            _count[station] += 1
            _recent[station].appendleft({"t": ts[11:19], "tote": tote})
        recent, count = list(_recent[station]), _count[station]
    if dup:
        return jsonify(status="duplicate", recent=recent, count=count)
    emit(station, "scan", engraver=engraver, tote=tote)
    return jsonify(status="ok", recent=recent, count=count)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
