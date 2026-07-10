"""
Engraving Station Logger — Ikigai Cases.  (login/logout + tote scans + session stats)

A standalone system (NOT connected to ShipHero). Each engraving station opens a page on its
tablet. The engraver logs in (tap a name, or scan/type their badge), scans each tote as they
finish it, and logs out at the end. Everything is written to your own spreadsheet as one event
log:  ts, station, event, engraver, tote        (event = login | logout | scan)

Why: ShipHero attributes a pick to ONE person per order, so engraving credit can't live there.
This is decoupled from ShipHero. Login/logout gives engraver HOURS automatically; each tote scan
is stamped with the logged-in engraver (the output). The tablet shows the engraver their own live
stats (totes this shift, time on shift, totes/hour).

Env: STATIONS, ENGRAVERS (quick-pick names), SINK, GSHEET_WEBAPP_URL / CSV_PATH / GSHEET_*,
     DEDUP_SECONDS, PORT
"""
import os, json, queue, threading
from collections import deque, defaultdict
from datetime import datetime, timezone

from flask import Flask, request, jsonify, render_template_string, redirect
from sinks import make_sink, COLUMNS

app = Flask(__name__)
STATIONS = [s.strip() for s in os.environ.get(
    "STATIONS", "Engraving 1,Engraving 2,Engraving 3").split(",") if s.strip()]
ENGRAVERS = [n.strip() for n in os.environ.get(
    "ENGRAVERS", "Halil Gurler,Manu Bekele,Maurice Williams").split(",") if n.strip()]
DEDUP_SECONDS = int(os.environ.get("DEDUP_SECONDS", "45"))

_sink = make_sink()
_q: "queue.Queue" = queue.Queue()
_current = {}
_login_ts = {}
_recent = defaultdict(lambda: deque(maxlen=25))
_count = defaultdict(int)
_last = {}
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
 header h1{margin:0;font-size:28px}
 header .status{font-size:18px;opacity:.92}
 main{padding:24px;max-width:860px;margin:0 auto}
 .prompt{font-size:27px;font-weight:800;margin:4px 0 4px}
 .hint{font-size:17px;opacity:.7;margin-bottom:18px}
 .flash{font-size:22px;padding:14px 18px;border-radius:12px;margin:12px 0;min-height:26px}
 .ok{background:#123f24;color:#7CE0A0}.dup{background:#4a3a10;color:#ffd873}.err{background:#4a1620;color:#ff9aa8}
 .names{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:18px}
 .names button{flex:1;min-width:190px;font-size:25px;font-weight:800;padding:30px 10px;border:0;border-radius:16px;background:#24506f;color:#fff;cursor:pointer}
 .names button:active{background:#2E75B6}
 .row{display:flex;gap:12px;margin:6px 0 16px}
 input[type=text]{flex:1;font-size:30px;padding:18px;border-radius:14px;border:2px solid #2E75B6;background:#0b121a;color:#fff}
 .btn{font-size:22px;font-weight:700;padding:0 26px;border:0;border-radius:14px;background:#2E75B6;color:#fff;cursor:pointer}
 .btn.out{background:#5b3a44}
 .stats{display:flex;gap:14px;margin:6px 0 18px}
 .card{flex:1;background:#16222e;border-radius:16px;padding:18px 12px;text-align:center}
 .card .num{font-size:38px;font-weight:800;color:#7CE0A0;line-height:1.1}
 .card .lab{font-size:14px;opacity:.7;margin-top:6px}
 table{width:100%;border-collapse:collapse;font-size:19px;margin-top:8px}td,th{padding:11px 8px;border-bottom:1px solid #22303f;text-align:left}
 .muted{opacity:.6;font-size:15px}a{color:#8fc1ee}.hide{display:none}
 .divider{opacity:.55;font-size:15px;text-align:center;margin:2px 0 12px}
 .bar{display:flex;justify-content:flex-end;margin:12px 0}
</style></head><body>
<header><h1>{{station}}</h1><div class="status" id="status"></div></header>
<main>
  <div class="muted" style="margin-bottom:10px"><a href="/">&#8592; switch station</a></div>
  <div id="flash" class="flash"></div>

  <div id="loginBox">
    <div class="prompt">Who&#8217;s engraving at this station?</div>
    <div class="hint">Tap your name &#8212; or scan / type it below.</div>
    <div class="names" id="names"></div>
    <div class="divider">&#8212; or scan badge / type name &#8212;</div>
    <form id="loginForm" class="row"><input id="badge" type="text" placeholder="Scan badge or type name&#8230;" autofocus>
      <button class="btn" type="submit">Start</button></form>
  </div>

  <div id="scanBox" class="hide">
    <div class="prompt">Scan each tote as you finish engraving it</div>
    <div class="hint">One scan per finished tote. A repeat scan within a few seconds is ignored.</div>
    <form id="scanForm" class="row"><input id="tote" type="text" placeholder="Scan finished tote&#8230;">
      <button class="btn" type="submit">Log tote</button></form>
    <div class="stats">
      <div class="card"><div class="num" id="s_count">0</div><div class="lab">totes this shift</div></div>
      <div class="card"><div class="num" id="s_time">0:00</div><div class="lab">time on shift</div></div>
      <div class="card"><div class="num" id="s_rate">&#8212;</div><div class="lab">totes / hour</div></div>
    </div>
    <div class="bar"><button class="btn out" id="logout" type="button">Log out</button></div>
    <table><thead><tr><th>Time</th><th>Tote</th></tr></thead><tbody id="rows">
      <tr><td colspan="2" class="muted">no totes yet &#8212; scan your first finished tote</td></tr></tbody></table>
  </div>
</main>
<script>
const station = {{ station_json|safe }};
const ENGRAVERS = {{ engravers_json|safe }};
const $=id=>document.getElementById(id);
const flash=$('flash');
let sinceMs=null, sessionCount=0;
function setFlash(c,m){flash.className='flash '+c;flash.textContent=m;}
function api(p,b){return fetch(p,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)}).then(r=>r.json());}
function renderNames(){
  $('names').innerHTML = ENGRAVERS.map((n,i)=>'<button data-i="'+i+'">'+n+'</button>').join('');
  Array.from($('names').children).forEach(b=>b.addEventListener('click',()=>doLogin(ENGRAVERS[+b.getAttribute('data-i')])));
}
function fmt(ms){var s=Math.max(0,Math.floor(ms/1000)),h=Math.floor(s/3600),m=Math.floor(s%3600/60),ss=s%60;
  return h>0?(h+':'+String(m).padStart(2,'0')+':'+String(ss).padStart(2,'0')):(m+':'+String(ss).padStart(2,'0'));}
function tick(){
  if(sinceMs==null){$('s_time').textContent='0:00';$('s_rate').textContent='\\u2014';return;}
  var ms=Date.now()-sinceMs; $('s_time').textContent=fmt(ms);
  var hrs=ms/3600000; $('s_rate').textContent=(hrs>0.01&&sessionCount>0)?(sessionCount/hrs).toFixed(1):'\\u2014';
  $('s_count').textContent=sessionCount;
}
setInterval(tick,1000);
function setCount(c){sessionCount=c||0;$('s_count').textContent=sessionCount;tick();}
function showLoggedOut(){sinceMs=null;$('status').textContent='not logged in';$('scanBox').classList.add('hide');$('loginBox').classList.remove('hide');$('badge').focus();}
function showLoggedIn(name,since,recent,count){
  sinceMs = since?Date.parse(since):Date.now();
  $('status').innerHTML='&#9635; <b>'+name+'</b> &#8212; engraving';
  $('loginBox').classList.add('hide');$('scanBox').classList.remove('hide');
  setCount(count||0);render(recent||[]);tick();$('tote').focus();
}
function render(list){if(list.length)$('rows').innerHTML=list.map(x=>`<tr><td>${x.t}</td><td>${x.tote}</td></tr>`).join('');}
async function doLogin(name){name=(name||'').trim();if(!name)return;const j=await api('/login',{station,engraver:name});
  setFlash('ok','\\u2713 '+j.engraver+' \\u2014 you\\u2019re on. Now scan totes.');showLoggedIn(j.engraver,j.since,[],0);}
$('loginForm').addEventListener('submit',e=>{e.preventDefault();const b=$('badge').value.trim();$('badge').value='';doLogin(b);});
$('scanForm').addEventListener('submit',async e=>{e.preventDefault();const t=$('tote').value.trim();if(!t){$('tote').focus();return;}$('tote').value='';
  const j=await api('/log',{station,tote:t});
  if(j.status==='ok')setFlash('ok','\\u2713 tote '+t+' logged');
  else if(j.status==='duplicate')setFlash('dup','\\u21bb '+t+' \\u2014 just scanned, skipped');
  else if(j.status==='not_logged_in'){setFlash('err','\\u2715 tap your name first');showLoggedOut();return;}
  else setFlash('err','\\u2715 '+(j.message||'error'));
  if(j.recent)render(j.recent);if(j.count!=null)setCount(j.count);$('tote').focus();});
$('logout').addEventListener('click',async()=>{await api('/logout',{station});setFlash('ok','logged out \\u2014 nice work');showLoggedOut();});
document.addEventListener('click',()=>{($('scanBox').classList.contains('hide')?$('badge'):$('tote')).focus();});
renderNames();
async function poll(){try{const r=await fetch('/state?station='+encodeURIComponent(station));const j=await r.json();
  if(!j.engraver){showLoggedOut();}else{if(sinceMs==null)showLoggedIn(j.engraver,j.since,j.recent,j.count);else{setCount(j.count);render(j.recent||[]);}}}catch(e){}}
poll();setInterval(poll,20000);
</script></body></html>
"""

PICKER = """<!doctype html><meta charset=utf-8><title>Engraving Stations</title>
<style>body{font-family:system-ui,Arial;background:#0f1720;color:#e7eef7;text-align:center;padding:40px}
a{display:block;font-size:30px;background:#2E75B6;color:#fff;text-decoration:none;padding:24px;border-radius:14px;margin:14px auto;max-width:440px;font-weight:700}</style>
<h1>Pick this tablet&#8217;s station</h1>{{ links|safe }}"""

@app.get("/")
def picker():
    links = "".join(f'<a href="/scan?station={s.replace(" ","%20")}">{s}</a>' for s in STATIONS)
    return render_template_string(PICKER, links=links)

@app.get("/scan")
def scan_page():
    station = request.args.get("station", "")
    if station not in STATIONS: return redirect("/")
    return render_template_string(PAGE, station=station,
                                  station_json=json.dumps(station),
                                  engravers_json=json.dumps(ENGRAVERS))

@app.get("/health")
def health():
    return jsonify(status="ok", stations=STATIONS, engravers=ENGRAVERS,
                   logged_in={s: _current.get(s) for s in STATIONS}, queue=_q.qsize())

@app.get("/state")
def state():
    station = request.args.get("station", "")
    with _lock:
        return jsonify(engraver=_current.get(station), since=_login_ts.get(station),
                       count=_count.get(station, 0), recent=list(_recent.get(station, [])))

@app.post("/login")
def login():
    d = request.get_json(silent=True) or {}
    station, engraver = (d.get("station") or "").strip(), (d.get("engraver") or "").strip()
    if station not in STATIONS or not engraver:
        return jsonify(status="error", message="missing station or name"), 400
    ts = now_iso()
    with _lock:
        _current[station] = engraver; _login_ts[station] = ts
        _count[station] = 0; _recent[station].clear()
    emit(station, "login", engraver=engraver)
    return jsonify(status="ok", engraver=engraver, since=ts)

@app.post("/logout")
def logout():
    d = request.get_json(silent=True) or {}
    station = (d.get("station") or "").strip()
    with _lock:
        eng = _current.pop(station, ""); _login_ts.pop(station, None)
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
        ts = now_iso(); key = (station, tote); prev = _last.get(key)
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
