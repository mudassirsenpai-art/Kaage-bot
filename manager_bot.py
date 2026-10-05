#!/usr/bin/env python3
"""
Yomi Kaggle Manager Bot
=======================
/admin  ->  Accounts | Ipynb | Operate | GPU | Usage

- Accounts : Kaggle account add / delete (bot se token bhejo)
- Ipynb    : ipynb file add / remove (multiple). Variables ipynb ke andar hi set hote hain.
- Operate  : Start Bot / Stop Bot  (ipynb Kaggle accounts pe auto chalta hai, account khatam -> agla)
- GPU      : har ipynb ke liye GPU badlo (T4 / T4 Highmem / P100 / CPU only)
- Usage    : har account ke weekly GPU hours + kaun kya chala raha hai

ENV (sirf manager bot ke):
  API_ID, API_HASH, BOT_TOKEN, OWNER_ID
  optional: ADMINS="id1,id2"  DATA_DIR=data  PORT  GPU_WEEKLY_HOURS=30  MAX_RUN_HOURS=11.5
            POLL_SECONDS=60  COOLDOWN_HOURS=6  GAP_SECONDS=20  DEFAULT_GPU=t4 (t4|t4h|p100|cpu)
"""
import os, re, sys, json, time, html, shutil, random, string, asyncio, tempfile, subprocess, threading
import http.server
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup as IKM, InlineKeyboardButton as IKB
from pyrogram.errors import MessageNotModified

# ============================== config
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
ADMINS = {OWNER_ID} | {int(x) for x in os.environ.get("ADMINS", "").replace(" ", "").split(",") if x.lstrip("-").isdigit()}
ADMINS.discard(0)

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
NB_DIR = DATA_DIR / "notebooks"
STATE_FILE = DATA_DIR / "state.json"

POLL = int(os.environ.get("POLL_SECONDS", "60"))
WEEKLY_LIMIT_H = float(os.environ.get("GPU_WEEKLY_HOURS", "30"))
SAFETY_H = float(os.environ.get("QUOTA_SAFETY_HOURS", "1"))
MAX_RUN_H = float(os.environ.get("MAX_RUN_HOURS", "11.5"))
QUICK_FAIL_S = int(os.environ.get("QUICK_FAIL_SECONDS", "600"))
COOLDOWN_H = float(os.environ.get("COOLDOWN_HOURS", "6"))
GAP_S = int(os.environ.get("GAP_SECONDS", "20"))
# GPU choices (menu me 🎮 GPU se badalte hain). Kaggle ka default P100 hota hai, isliye T4 explicitly set hota hai.
GPU_CHOICES = {
    "t4": ("NvidiaTeslaT4", "T4"),
    "t4h": ("NvidiaTeslaT4Highmem", "T4 Highmem"),
    "p100": ("NvidiaTeslaP100", "P100"),
    "cpu": (None, "CPU only"),
}
DEFAULT_GPU = os.environ.get("DEFAULT_GPU", "t4") if os.environ.get("DEFAULT_GPU", "t4") in GPU_CHOICES else "t4"
PREFIX = "mgr-"                      # sab kernels is prefix se bante hain (cleanup ke liye)

# ============================== state
DATA_DIR.mkdir(parents=True, exist_ok=True)
NB_DIR.mkdir(parents=True, exist_ok=True)


def _load():
    try:
        s = json.loads(STATE_FILE.read_text())
    except Exception:
        s = {}
    s.setdefault("accounts", {})
    s.setdefault("notebooks", {})
    s.setdefault("seq", 0)
    return s


STATE = _load()


def save():
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(STATE))
    tmp.replace(STATE_FILE)


def new_id(prefix):
    STATE["seq"] += 1
    return f"{prefix}{STATE['seq']}"


JOBS = {}      # nb_id -> runtime dict (supervisor)
BUSY = set()   # account ids jo abhi kisi kernel me hain
AWAIT = {}     # user_id -> {"type": "acc_add" | "nb_add"}

esc = html.escape


def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)


# ============================== kaggle helpers (blocking - to_thread me chalao)
def kaggle(acc, args, timeout=120, stdin=None):
    cfg = Path(tempfile.gettempdir()) / f"kcfg_{acc['id']}"
    cfg.mkdir(parents=True, exist_ok=True)
    creds = cfg / "kaggle.json"
    creds.write_text(json.dumps({"username": acc["user"], "key": acc["key"]}))
    try:
        creds.chmod(0o600)
    except Exception:
        pass
    env = os.environ.copy()
    env.update(KAGGLE_CONFIG_DIR=str(cfg), KAGGLE_USERNAME=acc["user"], KAGGLE_KEY=acc["key"])
    if acc["key"].startswith("KGAT_"):                 # naya token format
        env["KAGGLE_API_TOKEN"] = acc["key"]
        (cfg / "access_token").write_text(acc["key"])
    return subprocess.run(["kaggle", *args], env=env, capture_output=True, text=True, timeout=timeout, input=stdin)


def validate_account(acc):
    try:
        r = kaggle(acc, ["kernels", "list", "--user", acc["user"], "--page-size", "1"], 60)
    except FileNotFoundError:
        return False, "kaggle CLI install nahi hai (pip install kaggle)"
    except Exception as e:
        return False, str(e)
    if r.returncode == 0:
        return True, ""
    return False, (r.stderr or r.stdout).strip()[-200:]


def delete_kernel(acc, slug):
    ref = f"{acc['user']}/{slug}"
    try:
        r = kaggle(acc, ["kernels", "delete", "-k", ref, "-y"], 60, stdin="y\n")
        if r.returncode != 0:
            r = kaggle(acc, ["kernels", "delete", "-k", ref], 60, stdin="y\n")
        return r.returncode == 0
    except Exception:
        return False


def cleanup_leftovers(acc):
    """Manager ke purane (mgr-*) kernels hatao taaki double instance na chale."""
    try:
        r = kaggle(acc, ["kernels", "list", "--user", acc["user"], "--csv"], 60)
        if r.returncode != 0:
            return 0
        n = 0
        for line in r.stdout.strip().splitlines()[1:]:
            ref = line.split(",")[0].strip()
            if ref.split("/")[-1].startswith(PREFIX) and delete_kernel(acc, ref.split("/")[-1]):
                n += 1
        return n
    except Exception:
        return 0


KEEPALIVE = '''#@title keep alive (manager)
# Background me start hue bot ke liye: session tab tak zinda jab tak bot process chal raha ho.
import time as _t
_p = globals().get("proc")
if _p is not None and hasattr(_p, "poll"):
    while _p.poll() is None:
        _t.sleep(30)
    raise RuntimeError(f"Bot process exit hua (code {_p.returncode})")
'''


def prepare_notebook(nb_id):
    nb = json.loads((NB_DIR / f"{nb_id}.ipynb").read_text(encoding="utf-8"))
    cells = []
    for c in nb.get("cells", []):
        if c.get("cell_type") == "code":
            src = c.get("source", "")
            src = "".join(src) if isinstance(src, list) else src
            head = src.lstrip().split("\n", 1)[0].lower()
            if "#@title" in head and ("stop bot" in head or "live log" in head):
                continue                                   # stop / live-logs cells nahi chahiye
            c["outputs"], c["execution_count"] = [], None
        cells.append(c)
    cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
                  "source": KEEPALIVE.splitlines(keepends=True)})
    nb["cells"] = cells
    nb.setdefault("metadata", {})
    nb["nbformat"] = nb.get("nbformat", 4)
    nb["nbformat_minor"] = nb.get("nbformat_minor", 5)
    return nb


def push_kernel(acc, nb_id, slug):
    shape, _label = GPU_CHOICES[nb_gpu(nb_id)]
    wd = Path(tempfile.mkdtemp(prefix="push_"))
    try:
        (wd / "notebook.ipynb").write_text(json.dumps(prepare_notebook(nb_id)), encoding="utf-8")
        meta = {"id": f"{acc['user']}/{slug}", "title": slug, "code_file": "notebook.ipynb",
                "language": "python", "kernel_type": "notebook", "is_private": True,
                "enable_gpu": shape is not None, "enable_internet": True,
                "dataset_sources": [], "competition_sources": [], "kernel_sources": [], "model_sources": []}
        if shape:
            meta["machine_shape"] = shape
        (wd / "kernel-metadata.json").write_text(json.dumps(meta))
        r = kaggle(acc, ["kernels", "push", "-p", str(wd)], 180)
        return r.returncode == 0, (r.stdout + r.stderr).strip()
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def kernel_status(acc, slug):
    """running | queued | complete | error | cancel | unknown"""
    try:
        r = kaggle(acc, ["kernels", "status", f"{acc['user']}/{slug}"], 60)
    except Exception:
        return "unknown"
    out = (r.stdout + r.stderr).lower()
    m = re.search(r'status\s+"?([a-z_.]+)"?', out)
    word = m.group(1) if m else out
    for key in ("complete", "error", "cancel", "running", "queued"):
        if key in word:
            return key
    return "unknown"


def nb_gpu(nb_id):
    """Is ipynb ka GPU code (t4/t4h/p100/cpu)."""
    code = STATE["notebooks"].get(nb_id, {}).get("gpu") or STATE.get("default_gpu") or DEFAULT_GPU
    return code if code in GPU_CHOICES else DEFAULT_GPU


# ============================== usage / account picking
def used_hours(aid, now=None):
    now = now or time.time()
    week_ago = now - 7 * 86400
    tot = 0.0
    for s, e, *_ in STATE["accounts"][aid].get("runs", []):
        s = max(s, week_ago)
        if e > s:
            tot += e - s
    return tot / 3600


def record_run(aid, start, end, gpu=True):
    a = STATE["accounts"].get(aid)
    if not a:
        return
    runs = [r for r in a.get("runs", []) if r[1] > time.time() - 8 * 86400]
    if gpu:                                   # CPU-only run GPU quota use nahi karta
        runs.append([start, end])
    a["runs"] = runs
    a["total_runs"] = a.get("total_runs", 0) + 1
    save()


def set_cooldown(aid, hours=COOLDOWN_H):
    a = STATE["accounts"].get(aid)
    if a:
        a["cooldown_until"] = time.time() + hours * 3600
        save()


def pick_account(gpu=True):
    now = time.time()
    ok = []
    for aid, a in STATE["accounts"].items():
        if aid in BUSY or a.get("cooldown_until", 0) > now:
            continue
        h = used_hours(aid, now)
        if gpu and h >= WEEKLY_LIMIT_H - SAFETY_H:
            continue
        ok.append((h, aid))
    if not ok:
        return None
    ok.sort()
    return ok[0][1]


# ============================== bot + notify
app = Client("YomiManager", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
             parse_mode=ParseMode.HTML, in_memory=True, workers=8)


async def notify(text):
    for uid in ADMINS:
        try:
            await app.send_message(uid, text)
        except Exception:
            pass


# ============================== supervisor (ek ipynb = ek supervisor)
async def sleep_check(job, secs):
    for _ in range(int(secs)):
        if job["stop"]:
            return
        await asyncio.sleep(1)


async def monitor(job, acc, slug, start):
    unknown, last = 0, 0.0
    while True:
        await asyncio.sleep(3)
        if job["stop"]:
            return "stopped"
        if job.get("abort"):
            return "aborted"
        now = time.time()
        if now - start > MAX_RUN_H * 3600:
            return "max_run"
        if now - last < POLL:
            continue
        last = now
        st = await asyncio.to_thread(kernel_status, acc, slug)
        if st in ("running", "queued"):
            job["kstate"] = st
            unknown = 0
            continue
        if st == "unknown":
            unknown += 1
            if unknown >= 5:
                return "status_unreachable"
            continue
        return st


async def supervise(nb_id):
    job = JOBS[nb_id]
    name = STATE["notebooks"][nb_id]["name"]
    told_wait = False
    try:
        while not job["stop"]:
            gpu_code = nb_gpu(nb_id)
            needs_gpu = GPU_CHOICES[gpu_code][0] is not None
            aid = pick_account(needs_gpu)
            if not aid:
                job["status"] = "waiting"
                if not told_wait:
                    told_wait = True
                    await notify(f"⏳ <b>{esc(name)}</b>: koi free Kaggle account nahi (busy/quota/cooldown). Wait kar raha hoon.")
                await sleep_check(job, 30)
                continue
            told_wait = False
            acc = dict(STATE["accounts"][aid], id=aid)
            slug = f"{PREFIX}{nb_id}-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
            BUSY.add(aid)
            job.update(acc=aid, status="pushing", start=None, abort=False, slug=slug, kstate=None, gpu=gpu_code)
            start, reason = time.time(), None
            try:
                ok, msg = await asyncio.to_thread(push_kernel, acc, nb_id, slug)
                if not ok:
                    set_cooldown(aid, 1)
                    await notify(f"❌ <b>{esc(name)}</b>: push fail ({esc(acc['user'])})\n<code>{esc(msg[-300:])}</code>")
                    reason = "push_fail"
                else:
                    start = time.time()
                    job.update(start=start, status="running")
                    h = used_hours(aid) if aid in STATE["accounts"] else 0
                    await notify(f"🚀 <b>{esc(name)}</b> start → <code>{esc(acc['user'])}</code> · {GPU_CHOICES[gpu_code][1]} (weekly used {h:.1f}h)")
                    reason = await monitor(job, acc, slug, start)
                    end = time.time()
                    record_run(aid, start, end, needs_gpu)
                    await asyncio.to_thread(delete_kernel, acc, slug)
            finally:
                BUSY.discard(aid)
                job.update(acc=None, start=None, status="idle")

            if job["stop"]:
                break
            if reason == "push_fail":
                await sleep_check(job, 15)
                continue
            mins = (time.time() - start) / 60
            if reason not in ("aborted",) and (time.time() - start) < QUICK_FAIL_S:
                set_cooldown(aid)
                await notify(f"⚠️ <b>{esc(name)}</b>: {esc(acc['user'])} sirf {mins:.0f} min chala ({reason}) → {COOLDOWN_H:g}h cooldown. Agla account try ho raha hai.")
            else:
                await notify(f"🔄 <b>{esc(name)}</b>: {esc(acc['user'])} band ({reason}, {mins:.0f} min). Agla account start ho raha hai.")
            await sleep_check(job, GAP_S)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log("supervisor crash", nb_id, repr(e))
        await notify(f"🚨 <b>{esc(name)}</b> supervisor crash: <code>{esc(repr(e))}</code>")
    finally:
        JOBS.pop(nb_id, None)


def start_nb(nb_id):
    if nb_id in JOBS or nb_id not in STATE["notebooks"]:
        return False
    STATE["notebooks"][nb_id]["desired"] = True
    save()
    JOBS[nb_id] = {"stop": False, "abort": False, "acc": None, "status": "starting", "start": None, "slug": None}
    JOBS[nb_id]["task"] = asyncio.create_task(supervise(nb_id))
    return True


async def stop_nb(nb_id):
    nb = STATE["notebooks"].get(nb_id)
    if nb:
        nb["desired"] = False
        save()
    job = JOBS.get(nb_id)
    if not job:
        return False
    job["stop"] = True
    try:
        await asyncio.wait_for(asyncio.shield(job["task"]), timeout=120)
    except Exception:
        job["task"].cancel()
    return True


# ============================== UI helpers
def admin_f(_, __, m):
    u = getattr(m, "from_user", None)
    return bool(u and u.id in ADMINS)


admin = filters.create(admin_f)


async def edit(q, text, kb=None):
    try:
        await q.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    except MessageNotModified:
        pass
    except Exception as e:
        log("edit fail", e)


def back(to="main"):
    return [IKB("⬅️ Back", callback_data=f"m:{to}")]


def main_kb():
    return IKM([
        [IKB("👤 Accounts", callback_data="m:acc"), IKB("📓 Ipynb", callback_data="m:nb")],
        [IKB("⚙️ Operate", callback_data="m:op"), IKB("🎮 GPU", callback_data="m:gpu")],
        [IKB("📊 Usage", callback_data="m:use")],
    ])


def main_text():
    return (f"🛠 <b>Admin Panel</b>\n\n👤 Accounts: <b>{len(STATE['accounts'])}</b>\n"
            f"📓 Ipynb: <b>{len(STATE['notebooks'])}</b>\n🟢 Running: <b>{len(JOBS)}</b>")


def bar(frac, n=10):
    f = max(0, min(n, int(round(frac * n))))
    return "█" * f + "░" * (n - f)


def acc_state(aid):
    for nb_id, j in JOBS.items():
        if j.get("acc") == aid:
            return f"🟢 {esc(STATE['notebooks'].get(nb_id, {}).get('name', '?'))}"
    a = STATE["accounts"][aid]
    now = time.time()
    if a.get("cooldown_until", 0) > now:
        return f"⏸ cooldown {(a['cooldown_until'] - now) / 3600:.1f}h"
    if used_hours(aid) >= WEEKLY_LIMIT_H - SAFETY_H:
        return "🚫 quota full"
    return "⚪ free"


def acc_menu():
    accs = STATE["accounts"]
    lines = ["👤 <b>Accounts</b>\n"]
    if not accs:
        lines.append("Abhi koi account nahi. ➕ Add dabao.")
    for i, (aid, a) in enumerate(accs.items(), 1):
        lines.append(f"{i}. <code>{esc(a['user'])}</code> — {acc_state(aid)}")
    kb = IKM([[IKB("➕ Add", callback_data="acc:add"), IKB("🗑 Delete", callback_data="acc:del")], back()])
    return "\n".join(lines), kb


def nb_menu():
    nbs = STATE["notebooks"]
    lines = ["📓 <b>Ipynb</b>\n"]
    if not nbs:
        lines.append("Abhi koi ipynb nahi. ➕ Add dabao.")
    for i, (nid, n) in enumerate(nbs.items(), 1):
        lines.append(f"{i}. <b>{esc(n['name'])}</b> {'🟢' if nid in JOBS else '⚪'}")
    kb = IKM([[IKB("➕ Add", callback_data="nb:add"), IKB("🗑 Remove", callback_data="nb:del")], back()])
    return "\n".join(lines), kb


def op_menu():
    nbs = STATE["notebooks"]
    lines = ["⚙️ <b>Operate</b>\n"]
    if not nbs:
        lines.append("Pehle Ipynb menu se ipynb add karo.")
    now = time.time()
    for nid, n in nbs.items():
        j = JOBS.get(nid)
        if not j:
            lines.append(f"⚪ <b>{esc(n['name'])}</b> — stopped · {GPU_CHOICES[nb_gpu(nid)][1]}")
            continue
        who = STATE["accounts"].get(j.get("acc"), {}).get("user")
        if j["status"] == "running" and j.get("start"):
            lines.append(f"🟢 <b>{esc(n['name'])}</b> — <code>{esc(who or '?')}</code> · {GPU_CHOICES[j.get('gpu') or nb_gpu(nid)][1]} · {(now - j['start']) / 3600:.1f}h")
        else:
            lines.append(f"🟡 <b>{esc(n['name'])}</b> — {j['status']}" + (f" · <code>{esc(who)}</code>" if who else ""))
    kb = IKM([[IKB("▶️ Start Bot", callback_data="op:start"), IKB("⏹ Stop Bot", callback_data="op:stop")],
              [IKB("🔄 Refresh", callback_data="m:op")], back()])
    return "\n".join(lines), kb


def gpu_menu():
    nbs = STATE["notebooks"]
    dflt = STATE.get("default_gpu") or DEFAULT_GPU
    lines = ["🎮 <b>GPU</b>\n", f"Default (naye ipynb): <b>{GPU_CHOICES[dflt][1]}</b>\n"]
    if not nbs:
        lines.append("Abhi koi ipynb nahi.")
    for nid, n in nbs.items():
        lines.append(f"• <b>{esc(n['name'])}</b> — {GPU_CHOICES[nb_gpu(nid)][1]}" + (" 🟢" if nid in JOBS else ""))
    lines.append("\nℹ️ Badlav agle start / account-switch se lagta hai. Abhi chalte bot pe turant lagana ho to Stop → Start karo.")
    rows = [[IKB(f"🎮 {n['name']} — {GPU_CHOICES[nb_gpu(nid)][1]}", callback_data=f"gpu:n:{nid}")] for nid, n in nbs.items()]
    if nbs:
        rows.append([IKB("⚙️ Set for ALL", callback_data="gpu:n:all")])
    rows.append(back())
    return "\n".join(lines), IKM(rows)


def usage_menu():
    lines = [f"📊 <b>Usage</b> (last 7 days, manager estimate / {WEEKLY_LIMIT_H:g}h)\n"]
    if not STATE["accounts"]:
        lines.append("Koi account nahi.")
    for aid, a in STATE["accounts"].items():
        h = used_hours(aid)
        lines.append(f"<code>{esc(a['user'])}</code>\n{bar(h / WEEKLY_LIMIT_H)} {h:.1f}h / {WEEKLY_LIMIT_H:g}h · runs: {a.get('total_runs', 0)}\n{acc_state(aid)}\n")
    return "\n".join(lines), IKM([[IKB("🔄 Refresh", callback_data="m:use")], back()])


def pick_list(items, prefix, extra=None):
    rows = [[IKB(label, callback_data=f"{prefix}:{key}")] for key, label in items]
    if extra:
        rows.append(extra)
    return rows


# ============================== commands
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(_, m):
    await m.reply_text("👋 Yomi Manager. Admin ke liye /admin" if m.from_user.id in ADMINS else "❌ Ye private manager bot hai.")


@app.on_message(filters.command("admin") & filters.private & admin)
async def admin_cmd(_, m):
    AWAIT.pop(m.from_user.id, None)
    await m.reply_text(main_text(), reply_markup=main_kb())


# ============================== callbacks
@app.on_callback_query(admin)
async def cb(_, q):
    d = q.data
    uid = q.from_user.id

    if d.startswith("m:"):
        AWAIT.pop(uid, None)
        page = d[2:]
        if page == "main":
            return await edit(q, main_text(), main_kb())
        if page == "acc":
            t, kb = acc_menu()
        elif page == "nb":
            t, kb = nb_menu()
        elif page == "op":
            t, kb = op_menu()
        elif page == "gpu":
            t, kb = gpu_menu()
        else:
            t, kb = usage_menu()
        await q.answer()
        return await edit(q, t, kb)

    # ---------- accounts
    if d == "acc:add":
        AWAIT[uid] = {"type": "acc_add"}
        return await edit(q, "➕ <b>Kaggle account add</b>\n\nToken bhejo is format me:\n<code>username:key</code>\n\n"
                             "ya <code>kaggle.json</code> ka poora content. Multiple accounts: har line me ek.\n"
                             "(Message add hote hi delete ho jayega.)", IKM([back("acc")]))
    if d == "acc:del":
        if not STATE["accounts"]:
            return await q.answer("Koi account nahi.", show_alert=True)
        items = [(aid, f"🗑 {a['user']}") for aid, a in STATE["accounts"].items()]
        return await edit(q, "🗑 Kaunsa account delete karna hai?", IKM(pick_list(items, "acc:delc", back("acc"))))
    if d.startswith("acc:delc:"):
        aid = d.split(":")[2]
        a = STATE["accounts"].get(aid)
        if not a:
            return await q.answer("Nahi mila.", show_alert=True)
        return await edit(q, f"⚠️ <code>{esc(a['user'])}</code> delete kar du?" + ("\nIspe bot chal raha hai, wo band hoke dusre account pe jayega." if aid in BUSY else ""),
                          IKM([[IKB("✅ Haan", callback_data=f"acc:delok:{aid}"), IKB("❌ Nahi", callback_data="m:acc")]]))
    if d.startswith("acc:delok:"):
        aid = d.split(":")[2]
        a = STATE["accounts"].get(aid)
        if a:
            for j in JOBS.values():
                if j.get("acc") == aid:
                    j["abort"] = True
            for _ in range(20):
                if aid not in BUSY:
                    break
                await asyncio.sleep(1)
            STATE["accounts"].pop(aid, None)
            save()
        await q.answer("Deleted ✅")
        t, kb = acc_menu()
        return await edit(q, t, kb)

    # ---------- ipynb
    if d == "nb:add":
        AWAIT[uid] = {"type": "nb_add"}
        return await edit(q, "➕ <b>Ipynb add</b>\n\n<code>.ipynb</code> file(s) bhejo (multiple bhej sakte ho). "
                             "Variables ipynb ke andar pehle se set hone chahiye.\nKhatam hone par ✅ Done dabao.",
                          IKM([[IKB("✅ Done", callback_data="m:nb")]]))
    if d == "nb:del":
        if not STATE["notebooks"]:
            return await q.answer("Koi ipynb nahi.", show_alert=True)
        items = [(nid, f"🗑 {n['name']}") for nid, n in STATE["notebooks"].items()]
        return await edit(q, "🗑 Kaunsa ipynb remove karna hai?", IKM(pick_list(items, "nb:delc", back("nb"))))
    if d.startswith("nb:delc:"):
        nid = d.split(":")[2]
        n = STATE["notebooks"].get(nid)
        if not n:
            return await q.answer("Nahi mila.", show_alert=True)
        return await edit(q, f"⚠️ <b>{esc(n['name'])}</b> remove kar du?" + ("\nYe abhi chal raha hai, pehle stop hoga." if nid in JOBS else ""),
                          IKM([[IKB("✅ Haan", callback_data=f"nb:delok:{nid}"), IKB("❌ Nahi", callback_data="m:nb")]]))
    if d.startswith("nb:delok:"):
        nid = d.split(":")[2]
        if nid in STATE["notebooks"]:
            await stop_nb(nid)
            STATE["notebooks"].pop(nid, None)
            save()
            try:
                (NB_DIR / f"{nid}.ipynb").unlink()
            except Exception:
                pass
        await q.answer("Removed ✅")
        t, kb = nb_menu()
        return await edit(q, t, kb)

    # ---------- gpu
    if d.startswith("gpu:n:"):
        nid = d.split(":")[2]
        title = "ALL ipynb" if nid == "all" else esc(STATE["notebooks"].get(nid, {}).get("name", "?"))
        rows = [[IKB(("✅ " if nid != "all" and nb_gpu(nid) == code else "") + label, callback_data=f"gpu:s:{nid}:{code}")]
                for code, (_shape, label) in GPU_CHOICES.items()]
        rows.append(back("gpu"))
        return await edit(q, f"🎮 <b>{title}</b> ke liye GPU chuno:", IKM(rows))
    if d.startswith("gpu:s:"):
        _, _, nid, code = d.split(":")
        if code not in GPU_CHOICES:
            return await q.answer()
        if nid == "all":
            STATE["default_gpu"] = code
            for n in STATE["notebooks"].values():
                n["gpu"] = code
        elif nid in STATE["notebooks"]:
            STATE["notebooks"][nid]["gpu"] = code
        save()
        await q.answer(f"GPU: {GPU_CHOICES[code][1]} ✅")
        t, kb = gpu_menu()
        return await edit(q, t, kb)

    # ---------- operate
    if d == "op:start":
        cand = [(nid, f"▶️ {n['name']}") for nid, n in STATE["notebooks"].items() if nid not in JOBS]
        if not STATE["accounts"]:
            return await q.answer("Pehle Accounts menu me Kaggle account add karo.", show_alert=True)
        if not cand:
            return await q.answer("Koi ipynb nahi ya sab chal rahe hain.", show_alert=True)
        extra = [IKB("▶️ Start All", callback_data="op:sa")] if len(cand) > 1 else None
        rows = pick_list(cand, "op:st") + ([extra] if extra else []) + [back("op")]
        return await edit(q, "▶️ Kaunsa bot start karna hai?", IKM(rows))
    if d.startswith("op:st:") or d == "op:sa":
        ids = [d.split(":")[2]] if d.startswith("op:st:") else [n for n in STATE["notebooks"] if n not in JOBS]
        started = sum(1 for n in ids if start_nb(n))
        await q.answer(f"{started} bot start ho raha hai 🚀")
        await asyncio.sleep(1)
        t, kb = op_menu()
        return await edit(q, t, kb)
    if d == "op:stop":
        cand = [(nid, f"⏹ {STATE['notebooks'][nid]['name']}") for nid in JOBS if nid in STATE["notebooks"]]
        if not cand:
            return await q.answer("Koi bot chal nahi raha.", show_alert=True)
        extra = [IKB("⏹ Stop All", callback_data="op:xa")] if len(cand) > 1 else None
        rows = pick_list(cand, "op:xs") + ([extra] if extra else []) + [back("op")]
        return await edit(q, "⏹ Kaunsa bot stop karna hai?", IKM(rows))
    if d.startswith("op:xs:") or d == "op:xa":
        ids = [d.split(":")[2]] if d.startswith("op:xs:") else list(JOBS)
        await q.answer("Stop ho raha hai... (kernel delete hone me thoda time lagta hai)")
        await edit(q, "⏹ Stopping...", None)
        await asyncio.gather(*(stop_nb(n) for n in ids))
        t, kb = op_menu()
        return await edit(q, t, kb)

    await q.answer()


# ============================== text / file capture
def parse_accounts(text):
    t = text.strip()
    items = []
    try:
        j = json.loads(t)
        for o in (j if isinstance(j, list) else [j]):
            items.append((str(o["username"]).strip(), str(o["key"]).strip()))
        return items
    except Exception:
        pass
    for line in t.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = re.split(r"[:\s,]+", line, maxsplit=1)
        if len(parts) == 2 and parts[0] and parts[1]:
            items.append((parts[0], parts[1].strip()))
    return items


@app.on_message(filters.text & filters.private & admin & ~filters.regex(r"^/"))
async def text_in(_, m):
    st = AWAIT.get(m.from_user.id)
    if not st or st["type"] != "acc_add":
        return
    items = parse_accounts(m.text)
    try:
        await m.delete()                                    # token chat me na rahe
    except Exception:
        pass
    if not items:
        return await m.reply_text("❌ Format galat. <code>username:key</code> bhejo (ya kaggle.json content).")
    out = []
    for user, key in items:
        if any(a["user"].lower() == user.lower() for a in STATE["accounts"].values()):
            out.append(f"⚠️ <code>{esc(user)}</code> pehle se added hai")
            continue
        aid = new_id("a")
        acc = {"id": aid, "user": user, "key": key}
        ok, err = await asyncio.to_thread(validate_account, acc)
        if ok:
            STATE["accounts"][aid] = {"user": user, "key": key, "runs": [], "total_runs": 0}
            save()
            out.append(f"✅ <code>{esc(user)}</code> added")
        else:
            out.append(f"❌ <code>{esc(user)}</code>: <code>{esc(err)}</code>")
    AWAIT.pop(m.from_user.id, None)
    t, kb = acc_menu()
    await m.reply_text("\n".join(out) + "\n\n" + t, reply_markup=kb)


@app.on_message(filters.document & filters.private & admin)
async def doc_in(_, m):
    st = AWAIT.get(m.from_user.id)
    if not st or st["type"] != "nb_add":
        return
    fname = m.document.file_name or "notebook.ipynb"
    if not fname.lower().endswith((".ipynb", ".txt", ".json")):
        return await m.reply_text("❌ Sirf .ipynb file bhejo.")
    try:
        buf = await m.download(in_memory=True)
        nb = json.loads(bytes(buf.getbuffer()).decode("utf-8"))
        assert isinstance(nb.get("cells"), list) and nb["cells"]
    except Exception:
        return await m.reply_text("❌ Ye valid ipynb nahi lag raha (cells nahi mile).")
    name = re.sub(r"(_ipynb)?\.(ipynb|txt|json)$", "", fname, flags=re.I)
    nid = new_id("n")
    (NB_DIR / f"{nid}.ipynb").write_text(json.dumps(nb), encoding="utf-8")
    STATE["notebooks"][nid] = {"name": name, "desired": False}
    save()
    await m.reply_text(f"✅ <b>{esc(name)}</b> added ({len(STATE['notebooks'])} total). Aur bhejo ya ✅ Done dabao.",
                       reply_markup=IKM([[IKB("✅ Done", callback_data="m:nb")]]))


# ============================== health server + startup
def health_server():
    port = os.environ.get("PORT")
    if not port:
        return

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers(); self.wfile.write(b"Yomi manager live")
        def log_message(self, *a):
            pass

    threading.Thread(target=lambda: http.server.HTTPServer(("0.0.0.0", int(port)), H).serve_forever(), daemon=True).start()


async def main():
    if not (API_ID and API_HASH and BOT_TOKEN and OWNER_ID):
        sys.exit("API_ID, API_HASH, BOT_TOKEN, OWNER_ID env set karo.")
    health_server()
    await app.start()
    log("Manager bot online")
    # purane leftover kernels saaf karo, phir jo 'desired' tha wo resume karo
    for aid, a in list(STATE["accounts"].items()):
        n = await asyncio.to_thread(cleanup_leftovers, dict(a, id=aid))
        if n:
            log(f"{a['user']}: {n} leftover kernel(s) deleted")
    resumed = [nid for nid, n in STATE["notebooks"].items() if n.get("desired") and start_nb(nid)]
    if resumed:
        await notify(f"♻️ Manager restart: {len(resumed)} bot resume ho rahe hain.")
    await idle()
    for nid in list(JOBS):
        JOBS[nid]["stop"] = True
    await asyncio.gather(*(j["task"] for j in list(JOBS.values())), return_exceptions=True)
    await app.stop()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
