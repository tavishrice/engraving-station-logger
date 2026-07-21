import os, json, threading
from collections import deque, defaultdict
from datetime import datetime, timezone

from flask import Flask, request, jsonify, render_template_string, redirect
from sinks import make_sink, COLUMNS
import fulfill_store

app = Flask(__name__)
STATIONS = [s.strip() for s in os.environ.get(
    "STATIONS", "Engraving 1,Engraving 2,Engraving 3").split(",") if s.strip()]
ENGRAVERS = [n.strip() for n in os.environ.get(
    "ENGRAVERS", "Halil Gurler,Manu Bekele,Maurice Williams").split(",") if n.strip()]
FULFILLERS = [n.strip() for n in os.environ.get(
    "FULFILLERS",
    "Nic Cox,Jeffrey Kwan,Kadil Ladson,Shambria Green,Manu Bekele,"
    "Halil Gurler,Maurice Williams,Esra Altug,Simay Guner").split(",") if n.strip()]
DEDUP_SECONDS = int(os.environ.get("DEDUP_SECONDS", "45"))
IDLE_SECONDS = int(float(os.environ.get("IDLE_MINUTES", "15")) * 60)
RECENT_MAX = int(os.environ.get("RECENT_MAX", "200"))

_sink = make_sink()
fulfill_store.start()               # background DB writer for the fulfillment flow

# ----- shared per-session state -----
# Engraving is keyed by physical STATION (the station matters at the carts).
# Fulfillment is keyed by a per-tablet DEVICE id (station is irrelevant to the
# packer; credit is by person). Both are just opaque keys into these dicts.
_current = {}       # key -> person
_login_ts = {}      # key -> iso login time
_last_active = {}   # key -> iso time of last real action (login/scan)
_recent = defaultdict(lambda: deque(maxlen=RECENT_MAX))
_count = defaultdict(int)
_last = {}          # (key, code) -> iso ts of last scan (rapid-dup guard)
_lock = threading.Lock()
_write_lock = threading.Lock()

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _is_idle(key):
    la = _last_active.get(key)
    if not la:
        return False
    return (datetime.now(timezone.utc) - datetime.fromisoformat(la)).total_seconds() > IDLE_SECONDS

def _clear(key):
    _current.pop(key, None); _login_ts.pop(key, None); _last_active.pop(key, None)

def emit(station, event, engraver="", tote="", note=""):
    """Write the event to the spreadsheet synchronously (reliable under gunicorn)."""
    row = {"ts": now_iso(), "station": station, "event": event,
           "engraver": engraver, "tote": tote, "source": "station-tablet", "note": note}
    with _write_lock:
        try:
            _sink.append_rows([row])
            print("[sink] wrote", event, station, tote, flush=True)
        except Exception as e:
            print("[sink] error:", repr(e), flush=True)

# ============================ shared scan page ============================
# One template drives both flows. Config is injected per-flow (accent, noun,
# endpoints, undo on/off, device-session on/off). Features: audible beep +
# haptic buzz on every scan, a big running total, a downward-growing scan log,
# offline-retry queue, double-submit guard, remember-this-tablet, and
# (fulfillment) undo-last.
SCAN_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{title_text}}</title>
<style>
 :root{color-scheme:dark;--accent:{{accent}};--accentd:{{accentd}}} *{box-sizing:border-box}
 body{margin:0;font-family:system-ui,Arial,sans-serif;background:#0f1720;color:#e7eef7}
 header{background:var(--accentd);padding:14px 20px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:5}
 header h1{margin:0;font-size:26px}
 header .status{font-size:17px;opacity:.92;text-align:right}
 main{padding:20px;max-width:900px;margin:0 auto}
 .prompt{font-size:26px;font-weight:800;margin:2px 0 4px}
 .hint{font-size:16px;opacity:.7;margin-bottom:16px}
 .flash{font-size:22px;font-weight:700;padding:16px 18px;border-radius:14px;margin:10px 0;min-height:30px;transition:background .1s}
 .ok{background:#123f24;color:#7CE0A0}.dup{background:#4a3a10;color:#ffd873}.err{background:#4a1620;color:#ff9aa8}
 .names{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:18px}
 .names button{flex:1;min-width:200px;font-size:26px;font-weight:800;padding:34px 10px;border:0;border-radius:16px;background:#24506f;color:#fff;cursor:pointer}
 .names button:active{background:var(--accent)}
 .row{display:flex;gap:12px;margin:6px 0 14px}
 input[type=text]{flex:1;font-size:32px;padding:20px;border-radius:14px;border:3px solid var(--accent);background:#0b121a;color:#fff}
 input[type=text]:focus{outline:none;border-color:#fff}
 .btn{font-size:22px;font-weight:800;padding:0 26px;border:0;border-radius:14px;background:var(--accent);color:#fff;cursor:pointer}
 .btn.out{background:#5b3a44}.btn.undo{background:#3a3f5b}
 .stats{display:flex;gap:14px;margin:6px 0 14px}
 .card{flex:1;background:#16222e;border-radius:16px;padding:16px 12px;text-align:center}
 .card.total{background:linear-gradient(160deg,#173026,#16222e);border:1px solid #2b4a3c}
 .card .num{font-size:52px;font-weight:900;color:#7CE0A0;line-height:1}
 .card .lab{font-size:13px;opacity:.7;margin-top:6px;text-transform:uppercase;letter-spacing:.05em}
 .bar{display:flex;justify-content:space-between;align-items:center;gap:12px;margin:6px 0 10px}
 .pending{color:#ffd873;font-size:15px;font-weight:700;min-height:20px}
 .logwrap{background:#0d1620;border:1px solid #22303f;border-radius:14px;overflow:hidden}
 .logwrap .cap{padding:10px 14px;font-size:13px;text-transform:uppercase;letter-spacing:.05em;opacity:.7;border-bottom:1px solid #22303f}
 .logbox{max-height:46vh;overflow-y:auto}
 table{width:100%;border-collapse:collapse;font-size:19px}td,th{padding:11px 12px;border-bottom:1px solid #1b2733;text-align:left}
 td.idx{color:#7CE0A0;font-weight:800;width:46px}
 tr:last-child td{background:#12261c}
 .muted{opacity:.55;font-size:15px}a{color:#8fc1ee}.hide{display:none}
 .divider{opacity:.55;font-size:14px;text-align:center;margin:2px 0 12px}
</style></head><body>
<header><h1>{{title_text}}</h1><div class="status" id="status"></div></header>
<main>
  <div class="muted" style="margin-bottom:10px"><a href="{{back_href}}">&#8592; {{back_label}}</a></div>
  <div id="flash" class="flash"></div>

  <div id="loginBox">
    <div class="prompt">Who&#8217;s {{role_prompt}}?</div>
    <div class="hint">Tap your name &#8212; or scan / type it below.</div>
    <div class="names" id="names"></div>
    <div class="divider">&#8212; or scan badge / type name &#8212;</div>
    <form id="loginForm" class="row"><input id="badge" type="text" placeholder="Scan badge or type name&#8230;" autofocus>
      <button class="btn" type="submit">Start</button></form>
  </div>

  <div id="scanBox" class="hide">
    <div class="prompt">Scan each {{noun}} as you {{scan_verb}} it</div>
    <div class="hint">{{scan_hint}} Auto logs out after {{idle_min}} min idle.</div>
    {% if mode=='fulfillment' %}
    <div style="margin:4px 0 14px;padding:14px 16px;border:1px solid #2b4a3c;border-radius:14px;background:linear-gradient(160deg,#14261d,#152230)">
      <div style="font-size:18px;font-weight:800;color:#7CE0A0;margin-bottom:8px">Scan whichever barcode reads &#8212; both work:</div>
      <div style="font-size:17px;margin:4px 0"><b style="color:#7CE0A0">1.</b>&#160; the <b>order number</b> on the packing slip (IC + 6 digits)</div>
      <div style="font-size:17px;margin:4px 0"><b style="color:#7CE0A0">2.</b>&#160; or the <b>shipping-label tracking</b> barcode</div>
      <div class="muted" style="margin-top:8px">If neither one scans, it&#8217;ll ask you to type the order number.</div>
    </div>
    {% endif %}
    <form id="scanForm" class="row"><input id="scan" type="text" placeholder="Scan {{noun}}&#8230;" autocomplete="off">
      <button class="btn" type="submit">Log {{noun}}</button></form>
    <div id="manualBox" class="hide" style="margin:6px 0 12px;padding:14px;border:1px solid #7a5;border-radius:12px;background:#1a2a1e">
      <div class="prompt" style="font-size:19px;margin:0 0 4px">That didn&#8217;t read as a full order&#160;#</div>
      <div class="hint" style="margin:0 0 8px">Scanned &#8220;<span id="mRaw"></span>&#8221;. Type the order number from the packing slip (IC&#160;+&#160;6&#160;digits).</div>
      <form id="manualForm" class="row"><input id="manual" type="text" autocomplete="off" placeholder="e.g. IC201854">
        <button class="btn" type="submit">Confirm order</button></form>
      <div class="muted" style="margin-top:8px"><a href="#" id="mCancel">cancel &#8212; back to scanning</a></div>
    </div>
    <div class="stats">
      <div class="card total"><div class="num" id="s_count">0</div><div class="lab">{{noun_plural}} {{ 'today' if mode=='fulfillment' else 'this shift' }}</div></div>
      <div class="card"><div class="num" id="s_time">0:00</div><div class="lab">time on shift</div></div>
      <div class="card"><div class="num" id="s_rate">&#8212;</div><div class="lab">{{noun_plural}} / hour</div></div>
    </div>
    <div class="bar">
      <div class="pending" id="pending"></div>
      <div>
        {% if undo_enabled %}<button class="btn undo" id="undo" type="button">&#8630; Undo last</button>{% endif %}
        <button class="btn out" id="logout" type="button">Log out</button>
      </div>
    </div>
    <div class="logwrap">
      <div class="cap">Scan log &#8212; newest at the bottom</div>
      <div class="logbox" id="logbox">
        <table><tbody id="rows"><tr><td colspan="3" class="muted" style="padding:16px">no {{noun_plural}} yet &#8212; scan your first</td></tr></tbody></table>
      </div>
    </div>
  </div>
</main>
<script>
function deviceSid(){try{var k='ik_fulfill_sid';var v=localStorage.getItem(k);if(!v){v='dev-'+Math.random().toString(36).slice(2,10)+Date.now().toString(36);localStorage.setItem(k,v);}return v;}catch(e){return 'dev-x';}}
const CFG={station:{{ 'null' if device_session else (station|tojson) }}, deviceSession:{{ 'true' if device_session else 'false' }},
  names:{{names|tojson}}, mode:{{mode|tojson}}, validateOrder:{{ 'true' if mode=='fulfillment' else 'false' }},
  epLogin:{{ep_login|tojson}}, epLog:{{ep_log|tojson}}, epLogout:{{ep_logout|tojson}},
  epState:{{ep_state|tojson}}, epUndo:{{ep_undo|tojson}}, scanKey:{{scan_key|tojson}},
  noun:{{noun|tojson}}, nounPlural:{{noun_plural|tojson}}};
if(CFG.deviceSession)CFG.station=deviceSid();
const $=id=>document.getElementById(id);
const flash=$('flash');
let sinceMs=null, sessionCount=0, loggedIn=false;
try{localStorage.setItem('ik_last',JSON.stringify({mode:CFG.mode,station:{{ title_text|tojson }},url:location.pathname+location.search}));}catch(e){}

// --- audio + haptic feedback ---
let AC=null;
function ensureAudio(){try{if(!AC)AC=new (window.AudioContext||window.webkitAudioContext)();if(AC.state==='suspended')AC.resume();}catch(e){}}
function tone(freq,dur,type,vol){if(!AC)return;try{const o=AC.createOscillator(),g=AC.createGain();o.type=type||'sine';o.frequency.value=freq;o.connect(g);g.connect(AC.destination);const t=AC.currentTime;g.gain.setValueAtTime(vol||0.16,t);g.gain.exponentialRampToValueAtTime(0.0001,t+dur);o.start(t);o.stop(t+dur);}catch(e){}}
function buzz(p){try{if(navigator.vibrate)navigator.vibrate(p);}catch(e){}}
function feedback(kind){
  if(kind==='ok'){tone(880,0.09,'sine',0.18);buzz(45);}
  else if(kind==='dup'){tone(440,0.15,'square',0.13);buzz([25,45,25]);}
  else if(kind==='undo'){tone(720,0.06,'sine',0.15);setTimeout(function(){tone(360,0.10,'sine',0.15);},60);buzz(70);}
  else{tone(200,0.30,'sawtooth',0.20);buzz(230);}
}

function setFlash(c,m){flash.className='flash '+c;flash.textContent=m;}
function api(p,b){return fetch(p,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)}).then(r=>r.json());}
function renderNames(){
  $('names').innerHTML=CFG.names.map((n,i)=>'<button data-i="'+i+'">'+n+'</button>').join('');
  Array.from($('names').children).forEach(b=>b.addEventListener('click',()=>doLogin(CFG.names[+b.getAttribute('data-i')])));
}
function fmt(ms){var s=Math.max(0,Math.floor(ms/1000)),h=Math.floor(s/3600),m=Math.floor(s%3600/60),ss=s%60;
  return h>0?(h+':'+String(m).padStart(2,'0')+':'+String(ss).padStart(2,'0')):(m+':'+String(ss).padStart(2,'0'));}
function tick(){
  if(sinceMs==null){$('s_time').textContent='0:00';$('s_rate').textContent='\\u2014';return;}
  var ms=Date.now()-sinceMs;$('s_time').textContent=fmt(ms);
  var hrs=ms/3600000;$('s_rate').textContent=(hrs>0.01&&sessionCount>0)?(sessionCount/hrs).toFixed(1):'\\u2014';
  $('s_count').textContent=sessionCount;
}
setInterval(tick,1000);
function setCount(c){sessionCount=c||0;$('s_count').textContent=sessionCount;tick();}
function showLoggedOut(){loggedIn=false;sinceMs=null;$('status').textContent='not logged in';$('scanBox').classList.add('hide');$('loginBox').classList.remove('hide');$('badge').focus();}
function showLoggedIn(name,since,recent,count){
  loggedIn=true;sinceMs=since?Date.parse(since):Date.now();
  $('status').innerHTML='&#9635; <b>'+name+'</b>';
  $('loginBox').classList.add('hide');$('scanBox').classList.remove('hide');
  setCount(count||0);render(recent||[]);tick();$('scan').focus();
}
function render(list){
  var rows=$('rows');
  if(!list||!list.length){rows.innerHTML='<tr><td colspan="3" class="muted" style="padding:16px">no '+CFG.nounPlural+' yet \\u2014 scan your first</td></tr>';return;}
  var asc=list.slice().reverse(); // server sends newest-first; show oldest->newest (newest at bottom)
  rows.innerHTML=asc.map((x,i)=>'<tr><td class="idx">'+(i+1)+'</td><td>'+x.t+'</td><td>'+(x[CFG.scanKey]||'')+'</td></tr>').join('');
  var box=$('logbox');box.scrollTop=box.scrollHeight;
}
async function doLogin(name){ensureAudio();name=(name||'').trim();if(!name)return;const j=await api(CFG.epLogin,{station:CFG.station,engraver:name});
  feedback('ok');setFlash('ok','\\u2713 '+j.engraver+' \\u2014 you\\u2019re on. Now scan '+CFG.nounPlural+'.');showLoggedIn(j.engraver,j.since,[],0);}
$('loginForm').addEventListener('submit',e=>{e.preventDefault();const b=$('badge').value.trim();$('badge').value='';doLogin(b);});

// --- scanning with double-submit guard + offline retry ---
var lastVal='',lastT=0;
var pending=[];var retryTimer=null;
function pendBadge(){$('pending').textContent=pending.length?('\\u27f3 '+pending.length+' pending \\u2014 retrying'):'';}
function ensureRetry(){if(!retryTimer)retryTimer=setInterval(flushPending,3000);}
async function flushPending(){
  if(!pending.length){clearInterval(retryTimer);retryTimer=null;pendBadge();return;}
  var val=pending[0];
  try{const j=await api(CFG.epLog,scanBody(val));pending.shift();pendBadge();handleResp(j,val);}catch(e){}
}
function scanBody(val){var b={station:CFG.station};b[CFG.scanKey]=val;return b;}
function handleResp(j,val){
  if(j.status==='ok'){feedback('ok');setFlash('ok','\\u2713 '+CFG.noun+' '+val+' logged');}
  else if(j.status==='duplicate'){feedback('dup');setFlash('dup','\\u21bb '+val+' \\u2014 just scanned, skipped');}
  else if(j.status==='not_logged_in'){feedback('err');setFlash('err','\\u2715 logged out \\u2014 tap your name to start again');showLoggedOut();return;}
  else{feedback('err');setFlash('err','\\u2715 '+(j.message||'error'));}
  if(j.recent)render(j.recent);if(j.count!=null)setCount(j.count);
}
async function sendScan(val){
  ensureAudio();
  try{const j=await api(CFG.epLog,scanBody(val));handleResp(j,val);}
  catch(e){pending.push(val);pendBadge();ensureRetry();feedback('dup');setFlash('dup','\\u27f3 '+val+' saved \\u2014 offline, will retry ('+pending.length+')');}
}
// order-format check (Ikigai orders are IC + 6 digits; a bare 6-digit core is fine too).
function orderAlnum(v){return (v||'').toUpperCase().replace(/[^A-Z0-9]/g,'');}
function looksLikeOrder(v){return /^(IC)?[0-9]{6}$/.test(orderAlnum(v));}
function looksLikeTracking(v){return orderAlnum(v).length>=12;}  // shipping-label tracking barcode
var mBox=$('manualBox');
function showManual(raw){if(!mBox){sendScan(raw);return;}$('mRaw').textContent=raw;var m=$('manual');var d=(raw||'').replace(/[^0-9]/g,'');m.value=d?('IC'+d):'';$('scanForm').classList.add('hide');mBox.classList.remove('hide');feedback('err');setFlash('err','\\u2715 '+raw+' \\u2014 not a full order #, type it in');m.focus();m.select();}
function hideManual(){if(mBox)mBox.classList.add('hide');$('scanForm').classList.remove('hide');$('scan').focus();}
if($('manualForm')){$('manualForm').addEventListener('submit',function(e){e.preventDefault();var v=$('manual').value.trim();if(!looksLikeOrder(v)){feedback('err');setFlash('err','still not a 6-digit order \\u2014 check the packing slip');$('manual').focus();return;}var now=Date.now();lastVal=v;lastT=now;hideManual();sendScan(v);});}
if($('mCancel')){$('mCancel').addEventListener('click',function(e){e.preventDefault();hideManual();});}
$('scanForm').addEventListener('submit',e=>{e.preventDefault();var t=$('scan').value.trim();$('scan').value='';if(!t){$('scan').focus();return;}
  var now=Date.now();if(t===lastVal&&now-lastT<800){$('scan').focus();return;}lastVal=t;lastT=now;
  if(CFG.validateOrder&&!looksLikeOrder(t)&&!looksLikeTracking(t)){showManual(t);return;}
  sendScan(t);$('scan').focus();});

var undoBtn=$('undo');
if(undoBtn){undoBtn.addEventListener('click',async()=>{ensureAudio();
  try{const j=await api(CFG.epUndo,{station:CFG.station});
    if(j.status==='ok'){feedback('undo');setFlash('dup','\\u21b6 removed '+(j.removed||'last '+CFG.noun));}
    else{setFlash('dup','nothing to undo');}
    if(j.recent)render(j.recent);if(j.count!=null)setCount(j.count);}
  catch(e){feedback('err');setFlash('err','undo failed \\u2014 network');}
});}

$('logout').addEventListener('click',async()=>{loggedIn=false;await api(CFG.epLogout,{station:CFG.station});setFlash('ok','logged out \\u2014 nice work');showLoggedOut();});
window.addEventListener('pagehide',()=>{if(loggedIn){try{navigator.sendBeacon(CFG.epLogout,new Blob([JSON.stringify({station:CFG.station})],{type:'application/json'}));}catch(e){}}});
document.addEventListener('click',()=>{if(!AC)ensureAudio();($('scanBox').classList.contains('hide')?$('badge'):$('scan')).focus();});
renderNames();
async function poll(){try{const r=await fetch(CFG.epState+'?station='+encodeURIComponent(CFG.station));const j=await r.json();
  if(!j.engraver){if(loggedIn)setFlash('dup','logged out after inactivity \\u2014 tap your name to resume');showLoggedOut();}
  else{if(sinceMs==null)showLoggedIn(j.engraver,j.since,j.recent,j.count);else{setCount(j.count);render(j.recent||[]);}}}catch(e){}}
poll();setInterval(poll,20000);
</script></body></html>
"""

HOME = """<!doctype html><html><head><meta charset=utf-8><title>Ikigai Warehouse Logger</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
 :root{color-scheme:dark} body{margin:0;font-family:system-ui,Arial;background:radial-gradient(120% 90% at 50% 0%,#15202b,#0f1720);color:#e7eef7;min-height:100vh;text-align:center;padding:40px 20px}
 .brand{font-size:14px;letter-spacing:.22em;text-transform:uppercase;opacity:.6;margin-bottom:6px}
 h1{font-size:27px;margin:0 0 26px}
 .cards{display:flex;flex-direction:column;gap:16px;max-width:460px;margin:0 auto}
 a.card{display:block;text-decoration:none;color:#fff;padding:30px 24px;border-radius:18px;font-weight:800;font-size:30px;box-shadow:0 8px 24px rgba(0,0,0,.3)}
 a.eng{background:linear-gradient(150deg,#2E75B6,#1F3B57)} a.ful{background:linear-gradient(150deg,#2E9B6B,#14432f)}
 a.card .sub{display:block;font-size:15px;font-weight:500;opacity:.85;margin-top:6px}
 #resume{display:none;max-width:460px;margin:0 auto 14px} a.resume{display:block;background:#1c2a38;color:#8fc1ee;border:1px solid #2b4152;border-radius:14px;padding:14px;font-size:17px;font-weight:700;text-decoration:none}
</style></head><body>
<div class="brand">Ikigai Warehouse</div>
<h1>What are you logging on this tablet?</h1>
<div id="resume"></div>
<div class="cards">
  <a class="card eng" href="/engraving">Engraving<span class="sub">scan finished totes at the engraving carts</span></a>
  <a class="card ful" href="/fulfillment">Fulfillment<span class="sub">scan Shopify rush orders as you pack them</span></a>
</div>
<script>try{var l=JSON.parse(localStorage.getItem('ik_last')||'null');
if(l&&l.url){var d=document.getElementById('resume');d.style.display='block';
d.innerHTML='<a class="resume" href="'+l.url+'">&#8631; Resume: '+l.station+'</a>';}}catch(e){}</script>
</body></html>"""

PICKER = """<!doctype html><html><head><meta charset=utf-8><title>{{title}}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
 :root{color-scheme:dark} body{margin:0;font-family:system-ui,Arial;background:radial-gradient(120% 90% at 50% 0%,#15202b,#0f1720);color:#e7eef7;min-height:100vh;text-align:center;padding:40px 20px}
 h1{font-size:23px;margin:0 0 22px}
 .cards{display:flex;flex-direction:column;gap:14px;max-width:460px;margin:0 auto}
 a.st{display:block;font-size:30px;color:#fff;text-decoration:none;padding:28px;border-radius:16px;font-weight:800;background:{{accent}};box-shadow:0 6px 18px rgba(0,0,0,.3)}
 a.back{display:inline-block;margin-top:22px;background:transparent;color:#8fc1ee;font-size:16px;font-weight:600;text-decoration:none;padding:8px}
</style></head><body>
<h1>{{heading}}</h1><div class="cards">{{ links|safe }}</div>
<a class="back" href="/">&#8592; back</a></body></html>"""


def _scan(names, accent, accentd, mode, noun, noun_plural, role_prompt, scan_verb,
          scan_hint, ep_login, ep_log, ep_logout, ep_state, ep_undo, scan_key,
          back_href, back_label, title_text, undo_enabled, device_session, station=""):
    return render_template_string(
        SCAN_PAGE, names=names, accent=accent, accentd=accentd, mode=mode, noun=noun,
        noun_plural=noun_plural, role_prompt=role_prompt, scan_verb=scan_verb,
        scan_hint=scan_hint, ep_login=ep_login, ep_log=ep_log, ep_logout=ep_logout,
        ep_state=ep_state, ep_undo=ep_undo, scan_key=scan_key, back_href=back_href,
        back_label=back_label, title_text=title_text, undo_enabled=undo_enabled,
        device_session=device_session, station=station,
        idle_min=int(round(IDLE_SECONDS / 60)))


@app.get("/")
def home():
    return render_template_string(HOME)


@app.get("/engraving")
def picker():
    links = "".join(f'<a class="st" style="background:#2E75B6" href="/scan?station={s.replace(" ","%20")}">{s}</a>' for s in STATIONS)
    return render_template_string(PICKER, links=links, title="Engraving Stations",
                                  accent="#2E75B6", heading="Pick this tablet's engraving station")


@app.get("/scan")
def scan_page():
    station = request.args.get("station", "")
    if station not in STATIONS: return redirect("/engraving")
    return _scan(ENGRAVERS, "#2E75B6", "#1F3B57", "engraving", "tote", "totes",
                 "engraving at this station", "finish engraving",
                 "One scan per finished tote.", "/login", "/log", "/logout", "/state",
                 "", "tote", "/engraving", "switch station", station, False, False,
                 station=station)


@app.get("/fulfillment")
def fulfill_page():
    # No station picker: the packer just logs in and scans. Each tablet uses an
    # invisible per-device session id (see deviceSid in the page). Credit is by
    # person, so the station is irrelevant here.
    return _scan(FULFILLERS, "#2E9B6B", "#14432f", "fulfillment", "order", "orders",
                 "packing rush orders", "pack",
                 "Scan the order barcode OR the shipping-label tracking barcode for each order.",
                 "/fulfill/login", "/fulfill/log", "/fulfill/logout", "/fulfill/state",
                 "/fulfill/undo", "order", "/", "home", "Fulfillment", True, True)


@app.get("/health")
def health():
    return jsonify(status="ok", stations=STATIONS, engravers=ENGRAVERS, fulfillers=FULFILLERS,
                   sink=os.environ.get("SINK", "csv"), idle_minutes=IDLE_SECONDS // 60,
                   fulfill=fulfill_store.stats(), logged_in=dict(_current))


# ------------------------------- engraving -------------------------------
@app.get("/state")
def state():
    station = request.args.get("station", "")
    expired = None
    with _lock:
        if _current.get(station) and _is_idle(station):
            expired = _current.get(station); _clear(station)
        resp = dict(engraver=_current.get(station), since=_login_ts.get(station),
                    count=_count.get(station, 0), recent=list(_recent.get(station, [])))
    if expired:
        emit(station, "logout", engraver=expired, note="auto: inactivity")
    return jsonify(resp)

@app.post("/login")
def login():
    d = request.get_json(silent=True) or {}
    station, engraver = (d.get("station") or "").strip(), (d.get("engraver") or "").strip()
    if station not in STATIONS or not engraver:
        return jsonify(status="error", message="missing station or name"), 400
    ts = now_iso()
    with _lock:
        _current[station] = engraver; _login_ts[station] = ts; _last_active[station] = ts
        _count[station] = 0; _recent[station].clear()
    emit(station, "login", engraver=engraver)
    return jsonify(status="ok", engraver=engraver, since=ts)

@app.post("/logout")
def logout():
    d = request.get_json(silent=True) or {}
    station = (d.get("station") or "").strip()
    with _lock:
        eng = _current.get(station); _clear(station)
    if eng:
        emit(station, "logout", engraver=eng)
    return jsonify(status="ok")

@app.post("/log")
def log_scan():
    d = request.get_json(silent=True) or {}
    station, tote = (d.get("station") or "").strip(), (d.get("tote") or "").strip()
    if station not in STATIONS or not tote:
        return jsonify(status="error", message="missing station or tote"), 400
    expired = None; status = None; scan_eng = None; recent = None; count = None
    with _lock:
        eng = _current.get(station)
        if eng and _is_idle(station):
            expired = eng; _clear(station); eng = None
        if not eng:
            status = "not_logged_in"
        else:
            ts = now_iso(); key = (station, tote); prev = _last.get(key)
            dup = prev and (datetime.fromisoformat(ts) - datetime.fromisoformat(prev)).total_seconds() < DEDUP_SECONDS
            _last[key] = ts; _last_active[station] = ts
            if not dup:
                _count[station] += 1
                _recent[station].appendleft({"t": ts[11:19], "tote": tote})
            status = "duplicate" if dup else "ok"; scan_eng = eng
            recent, count = list(_recent[station]), _count[station]
    if expired:
        emit(station, "logout", engraver=expired, note="auto: inactivity")
    if status == "not_logged_in":
        return jsonify(status="not_logged_in"), 200
    if status == "duplicate":
        return jsonify(status="duplicate", recent=recent, count=count)
    emit(station, "scan", engraver=scan_eng, tote=tote)
    return jsonify(status="ok", recent=recent, count=count)


# ------------------------------ fulfillment ------------------------------
# Keyed by a per-tablet device id (sent as "station"); no fixed station list.
@app.get("/fulfill/state")
def f_state():
    key = request.args.get("station", "")
    with _lock:
        if _current.get(key) and _is_idle(key):
            _clear(key)
        who = _current.get(key)
    dbc = fulfill_store.day_count(who) if who else None   # reconcile tile to DB day total
    with _lock:
        if who and _current.get(key) == who and dbc is not None:
            _count[key] = max(dbc, _count.get(key, 0))     # never flicker backward on write lag
        resp = dict(engraver=_current.get(key), since=_login_ts.get(key),
                    count=_count.get(key, 0), recent=list(_recent.get(key, [])))
    return jsonify(resp)

@app.post("/fulfill/login")
def f_login():
    d = request.get_json(silent=True) or {}
    key, who = (d.get("station") or "").strip(), (d.get("engraver") or "").strip()
    if not key or not who:
        return jsonify(status="error", message="missing session or name"), 400
    seed = fulfill_store.day_count(who)          # per-person-per-day, not per-login
    ts = now_iso()
    with _lock:
        _current[key] = who; _login_ts[key] = ts; _last_active[key] = ts
        _count[key] = seed if seed is not None else 0; _recent[key].clear()
    print("[fulfill] login", who, "day_count=", seed, flush=True)
    return jsonify(status="ok", engraver=who, since=ts)

@app.post("/fulfill/logout")
def f_logout():
    d = request.get_json(silent=True) or {}
    key = (d.get("station") or "").strip()
    with _lock:
        who = _current.get(key); _clear(key)
    if who:
        print("[fulfill] logout", who, flush=True)
    return jsonify(status="ok")

@app.post("/fulfill/log")
def f_log():
    d = request.get_json(silent=True) or {}
    key, order = (d.get("station") or "").strip(), (d.get("order") or "").strip()
    if not key or not order:
        return jsonify(status="error", message="missing session or order"), 400
    status = None; who = None; recent = None; count = None
    with _lock:
        cur_who = _current.get(key)
        if cur_who and _is_idle(key):
            _clear(key); cur_who = None
        if not cur_who:
            status = "not_logged_in"
        else:
            ts = now_iso(); dkey = (key, order.upper()); prev = _last.get(dkey)
            dup = prev and (datetime.fromisoformat(ts) - datetime.fromisoformat(prev)).total_seconds() < DEDUP_SECONDS
            _last[dkey] = ts; _last_active[key] = ts
            if not dup:
                _count[key] += 1
                _recent[key].appendleft({"t": ts[11:19], "order": order})
            status = "duplicate" if dup else "ok"; who = cur_who
            recent, count = list(_recent[key]), _count[key]
    if status == "not_logged_in":
        return jsonify(status="not_logged_in"), 200
    if status == "duplicate":
        return jsonify(status="duplicate", recent=recent, count=count)
    fulfill_store.record(who, "fulfillment", order)   # -> background write to event
    return jsonify(status="ok", recent=recent, count=count)

@app.post("/fulfill/undo")
def f_undo():
    d = request.get_json(silent=True) or {}
    key = (d.get("station") or "").strip()
    if not key:
        return jsonify(status="error", message="missing session"), 400
    removed = None
    with _lock:
        cur_who = _current.get(key)
        rec = _recent.get(key)
        if cur_who and rec and len(rec) > 0:
            item = rec.popleft()                   # newest scan (appendleft)
            removed = item.get("order")
            if _count.get(key, 0) > 0:
                _count[key] -= 1
            if removed:
                _last.pop((key, removed.upper()), None)
            _last_active[key] = now_iso()
        recent = list(_recent.get(key, [])); count = _count.get(key, 0)
    if removed:
        fulfill_store.void(removed)                # -> background DELETE of that row
        print("[fulfill] undo", removed, flush=True)
    return jsonify(status="ok" if removed else "empty", removed=removed, recent=recent, count=count)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
