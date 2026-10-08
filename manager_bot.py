#!/usr/bin/env python3
"""
Yomi Kaggle Manager Bot
=======================
/admin  ->  Accounts | Ipynb | Operate | GPU | Usage

- Accounts : Kaggle account add / delete (bot se token bhejo)
- Ipynb    : ipynb file add / remove (multiple). Variables ipynb ke andar hi set hote hain.
- Operate  : Start Bot / Stop Bot / Kill All / 🔁 Auto Start ON-OFF  (ON: account khatam -> agla; OFF: ek run ke baad ruk jata hai)
- /kill    : sab bots stop + sab mgr-* kernels delete
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
UNKNOWN_MAX = int(os.environ.get("UNKNOWN_MAX_POLLS", "30"))   # itni baar lagatar status na mile tabhi "unreachable" (30 x 60s = ~30 min)
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
    s.setdefault("acc_order", [])         # account sequence (pehle kaun use hoga)
    s.setdefault("pick_mode", "order")    # order = sequence ke hisaab se | least = sabse kam used pehle
    s.setdefault("auto_start", True)      # 🔁 Auto Start ON/OFF (Operate menu)
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
    cmd = [sys.executable, "-c", args[1], *args[2:]] if args and args[0] == "__py__" else ["kaggle", *args]
    return subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout, input=stdin)


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


PY_DELETE = r"""
import sys
ref = sys.argv[1]; user, slug = ref.split("/", 1)
from kaggle.api.kaggle_api_extended import KaggleApi
api = KaggleApi(); api.authenticate()
cands = [n for n in dir(api) if "delete" in n.lower() and "kernel" in n.lower()]
if not cands:
    print("SDK me kernel-delete method nahi. delete-wale methods:", [n for n in dir(api) if "delete" in n.lower()]); sys.exit(3)
fn = getattr(api, cands[0])
for a in ((ref,), (user, slug), (slug,)):
    try:
        fn(*a); print("OK", cands[0]); sys.exit(0)
    except TypeError:
        continue
    except Exception as e:
        print("ERR", repr(e)); sys.exit(4)
print("SDK signature mismatch", cands[0]); sys.exit(5)
"""


def kernel_gone(acc, slug):
    """Kaggle pe kernel ab nahi hai? True/False, None = verify nahi ho paya.
    Delete API kabhi 403 deta hai jabki kernel asal me delete ho chuka hota hai (fake error) - isliye list se verify."""
    slugs, err = list_mgr_kernels(acc)
    if err:
        return None
    if slug in slugs:
        return False
    try:
        r = kaggle(acc, ["kernels", "list", "--user", acc["user"], "-s", slug, "--page-size", "20", "--csv"], 60)
    except Exception:
        return None
    if r.returncode != 0:
        return None
    return slug not in r.stdout


def delete_kernel(acc, slug, tries=2):
    """Kernel delete. Kaggle CLI ke alag-alag versions ke liye kai tareeke try hote hain.
    CLI error (jaise 403) aaye to bhi list se check hota hai - kernel gayab hai to success maana jata hai.
    Returns (ok, err)."""
    ref = f"{acc['user']}/{slug}"
    variants = [["kernels", "delete", ref, "--yes"], ["kernels", "delete", ref],
                ["kernels", "delete", "-k", ref, "-y"], ["kernels", "delete", "-k", ref],
                ["__py__", PY_DELETE, ref]]
    errs = {}
    for i in range(tries):
        for args in variants:
            label = "sdk" if args[0] == "__py__" else " ".join(a for a in args if a != ref)
            try:
                r = kaggle(acc, args, 60, stdin="y\n")
            except Exception as e:
                errs[label] = str(e)[-120:]
                continue
            if r.returncode == 0:
                return True, ""
            out = (r.stderr or r.stdout).strip()
            low = out.lower()
            if "404" in low or "not found" in low or "does not exist" in low:
                return True, ""
            errs[label] = out.splitlines()[-1][-140:] if out else "(no output)"
            if "unrecognized arguments" in low or "invalid choice" in low or "usage:" in low or args[0] == "__py__":
                continue                      # ye tareeka is version me nahi, agla try karo
            break                             # asli error (auth/network) -> neeche verify
        time.sleep(3)
        gone = kernel_gone(acc, slug)         # fake 403 check
        if gone:
            return True, ""
        if gone is None:
            errs["verify"] = "list check fail"
        time.sleep(2)
    return False, " | ".join(f"[{k}] {v}" for k, v in errs.items())


def list_mgr_kernels(acc):
    """Account ke saare mgr-* kernels (slug list), err."""
    try:
        r = kaggle(acc, ["kernels", "list", "--user", acc["user"], "--page-size", "100", "--csv"], 60)
    except Exception as e:
        return [], f"{acc['user']}: {e}"
    if r.returncode != 0:
        return [], f"{acc['user']} list fail: {(r.stderr or r.stdout).strip()[-150:]}"
    slugs = []
    for line in r.stdout.strip().splitlines()[1:]:
        slug = line.split(",")[0].strip().split("/")[-1]
        if slug.startswith(PREFIX):
            slugs.append(slug)
    return slugs, None


def cleanup_leftovers(acc):
    """Manager ke purane (mgr-*) kernels hatao taaki double instance na chale."""
    slugs, err = list_mgr_kernels(acc)
    n = 0
    for slug in slugs:
        if delete_kernel(acc, slug)[0]:
            n += 1
    return n


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


def parse_status(out):
    """Sirf tab status do jab output me asli 'status ...' line ho. API error text (403/timeout) = unknown."""
    m = re.search(r'status\s+"?([a-z_.]+)', out)
    if not m:
        return "unknown"
    word = m.group(1)
    for key, val in (("complete", "complete"), ("error", "error"), ("cancel", "cancel"),
                     ("running", "running"), ("queued", "queued"), ("new", "queued")):
        if key in word:
            return val
    return "unknown"


def kernel_status(acc, slug):
    """running | queued | complete | error | cancel | unknown"""
    out = ""
    for attempt in range(2):                  # ek baar retry - temporary API glitch ke liye
        try:
            r = kaggle(acc, ["kernels", "status", f"{acc['user']}/{slug}"], 60)
            out = (r.stdout + r.stderr).lower()
            st = parse_status(out) if r.returncode == 0 else "unknown"
        except Exception as e:
            out, st = f"exc {e}".lower(), "unknown"
        if st != "unknown":
            return st
        time.sleep(3)
    log("status unknown:", acc["user"], slug, out.strip()[-200:])
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


def ordered_ids():
    """Accounts ka sequence (user ne jo set kiya), naye accounts end me."""
    order = [a for a in STATE.get("acc_order", []) if a in STATE["accounts"]]
    order += [a for a in STATE["accounts"] if a not in order]
    return order


def pick_account(gpu=True):
    now = time.time()
    ok = []
    for aid in ordered_ids():
        a = STATE["accounts"][aid]
        if aid in BUSY or a.get("cooldown_until", 0) > now:
            continue
        h = used_hours(aid, now)
        if gpu and h >= WEEKLY_LIMIT_H - SAFETY_H:
            continue
        ok.append((h, aid))
    if not ok:
        return None
    if STATE.get("pick_mode", "order") == "least":
        ok.sort(key=lambda x: x[0])
    return ok[0][1]                         # order mode: sequence me sabse pehla free account


def wait_reason():
    now = time.time()
    parts = []
    for aid in ordered_ids():
        a = STATE["accounts"][aid]
        if aid in BUSY:
            r = "busy"
        elif a.get("cooldown_until", 0) > now:
            r = f"cooldown {(a['cooldown_until'] - now) / 3600:.1f}h"
        elif used_hours(aid, now) >= WEEKLY_LIMIT_H - SAFETY_H:
            r = "quota full"
        else:
            r = "free"
        parts.append(f"{a['user']}: {r}")
    return " · ".join(parts) or "koi account add nahi"


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
    unknown, last, warned = 0, 0.0, False
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
            unknown, warned = 0, False
            continue
        if st == "unknown":
            unknown += 1
            if unknown >= 5 and not warned:
                warned = True
                await notify(f"⚠️ <b>{esc(STATE['notebooks'].get(job.get('nb'), {}).get('name', 'bot'))}</b>: Kaggle status check fail ho raha hai ({esc(acc['user'])}). "
                             f"Bot chal raha ho sakta hai - abhi band nahi kar raha, {UNKNOWN_MAX} baar fail hone tak wait.")
            if unknown >= UNKNOWN_MAX:
                return "status_unreachable"
            continue
        return st


def auto_on():
    return bool(STATE.get("auto_start", True))


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
                    await notify(f"⏳ <b>{esc(name)}</b>: koi free Kaggle account nahi. Wait kar raha hoon.\n<code>{esc(wait_reason())}</code>")
                await sleep_check(job, 30)
                continue
            told_wait = False
            acc = dict(STATE["accounts"][aid], id=aid)
            slug = f"{PREFIX}{nb_id}-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
            BUSY.add(aid)
            job.update(nb=nb_id, acc=aid, status="pushing", start=None, abort=False, slug=slug, kstate=None, gpu=gpu_code, kernel_live=True)
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
                    if job["stop"]:                       # stop push ke dauran dabaya tha
                        reason = "stopped"
                    else:
                        h = used_hours(aid) if aid in STATE["accounts"] else 0
                        await notify(f"🚀 <b>{esc(name)}</b> start → <code>{esc(acc['user'])}</code> · {GPU_CHOICES[gpu_code][1]} (weekly used {h:.1f}h)")
                        reason = await monitor(job, acc, slug, start)
                    record_run(aid, start, time.time(), needs_gpu)
            finally:
                # Kernel HAMESHA delete hoga (stop / cancel / error / max-run, sab me) - main.py wala delete logic
                job["status"] = "stopping"
                dok, derr = await asyncio.to_thread(delete_kernel, acc, slug)
                if not dok:
                    await notify(f"⚠️ <b>{esc(name)}</b>: kernel delete fail <code>{esc(acc['user'])}/{esc(slug)}</code>\n<code>{esc(derr)}</code>\nKaggle me manually delete karo ya /kill chalao." + ("\n(403 = token ko delete permission nahi lagti; kernel list me abhi bhi dikh raha hai)" if "403" in derr else ""))
                BUSY.discard(aid)
                job.update(acc=None, start=None, status="idle", kernel_live=False)

            if job["stop"]:
                break

            if reason == "push_fail":
                if not auto_on():
                    STATE["notebooks"][nb_id]["desired"] = False
                    save()
                    await notify(f"⏹ <b>{esc(name)}</b>: push fail hua aur 🔁 Auto Start OFF hai, isliye dobara try nahi kiya.")
                    break
                await sleep_check(job, 15)
                continue

            mins = (time.time() - start) / 60
            # cooldown sirf asli kernel failure pe (error/cancel/complete jaldi). status_unreachable/stopped/max_run pe NAHI.
            quick = reason in ("error", "cancel", "complete") and (time.time() - start) < QUICK_FAIL_S
            if quick:
                set_cooldown(aid)

            if not auto_on():                              # 🔁 Auto Start OFF -> agla run start nahi hoga
                STATE["notebooks"][nb_id]["desired"] = False
                save()
                await notify(f"⏹ <b>{esc(name)}</b>: {esc(acc['user'])} band ({reason}, {mins:.0f} min). 🔁 Auto Start OFF hai, isliye agla start nahi hua. Dobara chalane ke liye Operate → Start Bot.")
                break

            if quick:
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
    # Turant kernel delete (supervisor ka wait nahi) - main.py ke Cancel/kill jaisa
    if job.get("status") == "running" and job.get("acc") in STATE["accounts"] and job.get("slug"):
        acc = dict(STATE["accounts"][job["acc"]], id=job["acc"])
        await asyncio.to_thread(delete_kernel, acc, job["slug"])
    try:
        await asyncio.wait_for(asyncio.shield(job["task"]), timeout=240)
    except Exception:
        job["task"].cancel()
        await asyncio.gather(job["task"], return_exceptions=True)   # finally me kernel delete poora ho
    return True


async def kill_all():
    """main.py ke /kill jaisa: sab bots stop + sab accounts ke mgr-* kernels delete."""
    ids = list(JOBS)
    await asyncio.gather(*(stop_nb(n) for n in ids))
    deleted, errors = [], []
    for aid, a in list(STATE["accounts"].items()):
        acc = dict(a, id=aid)
        slugs, err = await asyncio.to_thread(list_mgr_kernels, acc)
        if err:
            errors.append(err)
            continue
        for slug in slugs:
            ok, derr = await asyncio.to_thread(delete_kernel, acc, slug)
            if ok:
                deleted.append(f"{a['user']}/{slug}")
            else:
                errors.append(f"{a['user']}/{slug}: {derr}")
    for n in STATE["notebooks"].values():
        n["desired"] = False
    save()
    return ids, deleted, errors


def kill_text(ids, deleted, errors):
    t = f"✅ <b>{len(ids)}</b> bot stop, <b>{len(deleted)}</b> Kaggle kernel delete."
    if deleted:
        t += "\n" + "\n".join(f"• <code>{esc(d)}</code>" for d in deleted[:15])
    if errors:
        t += "\n\n⚠️ <b>Errors:</b>\n" + "\n".join(f"• {esc(e)}" for e in errors[:6])
    return t


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
    mode = STATE.get("pick_mode", "order")
    lines = ["👤 <b>Accounts</b>\n"]
    if not accs:
        lines.append("Abhi koi account nahi. ➕ Add dabao.")
    for i, aid in enumerate(ordered_ids(), 1):
        lines.append(f"{i}. <code>{esc(accs[aid]['user'])}</code> — {acc_state(aid)}")
    if accs:
        lines.append(f"\n🔀 Pick mode: <b>{'Sequence (upar wala pehle)' if mode == 'order' else 'Least used pehle'}</b>")
    kb = IKM([[IKB("➕ Add", callback_data="acc:add"), IKB("🗑 Delete", callback_data="acc:del")],
              [IKB("🔀 Sequence", callback_data="acc:seq"), IKB("⏸ Reset Cooldown", callback_data="acc:cd")], back()])
    return "\n".join(lines), kb


def seq_menu():
    accs = STATE["accounts"]
    mode = STATE.get("pick_mode", "order")
    lines = ["🔀 <b>Account Sequence</b>\n", "Upar wala account pehle use hoga, wo busy/cooldown/quota me ho to agla.\n"]
    rows = []
    ids = ordered_ids()
    for i, aid in enumerate(ids, 1):
        lines.append(f"{i}. <code>{esc(accs[aid]['user'])}</code> — {acc_state(aid)}")
        rows.append([IKB(f"{i}. {accs[aid]['user']}", callback_data="seq:noop"),
                     IKB("🔼", callback_data=f"seq:up:{aid}"), IKB("🔽", callback_data=f"seq:dn:{aid}")])
    if not ids:
        lines.append("Koi account nahi.")
    rows.append([IKB(f"Mode: {'Sequence ✅' if mode == 'order' else 'Least used ✅'} (badlo)", callback_data="seq:mode")])
    rows.append(back("acc"))
    return "\n".join(lines), IKM(rows)


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
    lines.append(f"\n🔁 Auto Start: <b>{'ON ✅' if auto_on() else 'OFF ❌'}</b>"
                 + ("" if auto_on() else " — run khatam hone (≈12h) par agla start nahi hoga"))
    kb = IKM([[IKB("▶️ Start Bot", callback_data="op:start"), IKB("⏹ Stop Bot", callback_data="op:stop")],
              [IKB(f"🔁 Auto Start: {'ON ✅' if auto_on() else 'OFF ❌'}", callback_data="op:auto")],
              [IKB("💀 Kill All", callback_data="op:kill"), IKB("🔄 Refresh", callback_data="m:op")], back()])
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


@app.on_message(filters.command("kill") & filters.private & admin)
async def kill_cmd(_, m):
    if not STATE["accounts"]:
        return await m.reply_text("❌ Koi Kaggle account nahi hai.")
    msg = await m.reply_text("🗑️ Saare bots stop aur kernels delete kar raha hoon...")
    res = await kill_all()
    await msg.edit_text(kill_text(*res))


@app.on_message(filters.command("kdebug") & filters.private & admin)
async def kdebug_cmd(_, m):
    if not STATE["accounts"]:
        return await m.reply_text("❌ Koi Kaggle account nahi hai.")
    aid, a = next(iter(STATE["accounts"].items()))
    acc = dict(a, id=aid)
    out = []
    for args in (["--version"], ["kernels", "--help"], ["kernels", "delete", "--help"]):
        try:
            r = await asyncio.to_thread(kaggle, acc, args, 60)
            txt = ((r.stdout or "") + (r.stderr or "")).strip()
        except Exception as e:
            txt = str(e)
        out.append(f"<b>kaggle {' '.join(args)}</b>\n<pre>{esc(txt[-900:])}</pre>")
    await m.reply_text("\n".join(out))


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

    if d == "acc:seq":
        t, kb = seq_menu()
        return await edit(q, t, kb)
    if d == "acc:cd":
        n = 0
        for a in STATE["accounts"].values():
            if a.pop("cooldown_until", 0):
                n += 1
        save()
        await q.answer(f"{n} account ka cooldown hata diya ✅", show_alert=True)
        t, kb = acc_menu()
        return await edit(q, t, kb)
    if d.startswith("seq:"):
        parts = d.split(":")
        act = parts[1]
        if act == "mode":
            STATE["pick_mode"] = "least" if STATE.get("pick_mode", "order") == "order" else "order"
            save()
        elif act in ("up", "dn"):
            ids = ordered_ids()
            aid = parts[2]
            if aid in ids:
                i = ids.index(aid)
                j = i - 1 if act == "up" else i + 1
                if 0 <= j < len(ids):
                    ids[i], ids[j] = ids[j], ids[i]
                STATE["acc_order"] = ids
                save()
        await q.answer()
        t, kb = seq_menu()
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
    if d == "op:auto":
        STATE["auto_start"] = not auto_on()
        save()
        await q.answer("Auto Start ON ✅" if auto_on() else "Auto Start OFF ❌")
        t, kb = op_menu()
        return await edit(q, t, kb)
    if d == "op:kill":
        return await edit(q, "💀 Saare bots stop honge aur sab accounts ke <code>mgr-</code> kernels delete honge. Pakka?",
                          IKM([[IKB("✅ Haan, Kill", callback_data="op:killok"), IKB("❌ Nahi", callback_data="m:op")]]))
    if d == "op:killok":
        await q.answer("Kill ho raha hai...")
        await edit(q, "💀 Killing...", None)
        res = await kill_all()
        return await edit(q, kill_text(*res), IKM([[IKB("⚙️ Operate", callback_data="m:op")]]))
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
    if not auto_on():                      # Auto Start OFF -> manager restart par bhi apne aap start nahi
        for n in STATE["notebooks"].values():
            n["desired"] = False
        save()
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
