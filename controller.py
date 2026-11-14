#!/usr/bin/env python3
"""LoadSwarm controller — доска задач и дашборд для своих load-агентов.

Запуск на VPS:  LOADSWARM_TOKEN=... python3 controller.py --port 8080
Открыть http://<vps>:8080/ — ввести токен, создать задачу.
Агенты подключаются сами и забирают задачу.

ВНИМАНИЕ: используйте только для нагрузочного тестирования серверов,
которыми вы владеете или имеете разрешение тестировать.
"""
import argparse
import json
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="LoadSwarm")
TOKEN = os.environ.get("LOADSWARM_TOKEN", "")

AGENTS = {}       # agent_id -> {name, host, os, joined, last_seen, working}
ACTIVE = None     # {id, url, method, headers, body, concurrency, duration_s, timeout_s, created}
STATS = {}        # agent_id -> [StatRow]
HISTORY = []      # сводки завершённых задач (последние 20, с полными параметрами)
PRESETS = []      # пресеты конфигураций задач: [{id, name, url, method, ...}]
_presets_lock = threading.Lock()


def _presets_path() -> Path:
    return Path(__file__).resolve().parent / "presets.json"


def _presets_load() -> None:
    try:
        data = json.loads(_presets_path().read_text(encoding="utf-8"))
        if isinstance(data, list):
            PRESETS[:] = data
    except Exception:
        pass  # файла нет или JSON битый — начинаем с пустого списка


def _presets_save() -> None:
    try:
        _presets_path().write_text(
            json.dumps(PRESETS, ensure_ascii=False, indent=1), encoding="utf-8")
    except OSError as e:
        print(f"ПРЕДУПРЕЖДЕНИЕ: пресеты не сохранены на диск: {e}", flush=True)


_presets_load()


def auth(authorization: str = "", token: str = "") -> None:
    if not TOKEN:
        return
    if authorization == f"Bearer {TOKEN}" or token == TOKEN:
        return
    raise HTTPException(status_code=401, detail="bad token")


class Join(BaseModel):
    name: str
    host: str = ""
    os: str = ""
    py: str = ""


class JobIn(BaseModel):
    url: str
    method: str = "GET"
    concurrency: int = 10
    duration_s: float = 30.0
    timeout_s: float = 10.0
    rampup_s: float = 0.0          # разгон: concurrency наращивается линейно за это время
    body: Optional[str] = None
    headers: dict = {}
    agents: list = []              # agent_id участников; пусто = все подключённые агенты


class StatRow(BaseModel):
    job_id: str
    ts: int
    ok: int
    err: int
    p50: float
    p95: float
    p99: float
    avg: float
    codes: dict = {}


@app.post("/api/join")
def join(payload: Join, authorization: str = Header(default="")):
    auth(authorization)
    aid = uuid.uuid4().hex[:8]
    AGENTS[aid] = {
        "name": payload.name, "host": payload.host, "os": payload.os,
        "py": payload.py, "joined": time.time(), "last_seen": time.time(),
        "working": False,
    }
    return {"id": aid}


@app.get("/api/job/{agent_id}")
def get_job(agent_id: str, authorization: str = Header(default="")):
    auth(authorization)
    if agent_id not in AGENTS:
        raise HTTPException(status_code=404, detail="unknown agent")
    AGENTS[agent_id]["last_seen"] = time.time()
    if ACTIVE is None or agent_id in ACTIVE.get("done", set()):
        return Response(status_code=204)
    if ACTIVE.get("agents") and agent_id not in ACTIVE["agents"]:
        return Response(status_code=204)  # агент не выбран для этой задачи
    return active_view()


def active_view():
    """ACTIVE без служебных полей (для ответов агентам и дашборду)."""
    return {k: v for k, v in ACTIVE.items() if k not in ("done", "agents")}


@app.post("/api/stats/{agent_id}")
def stats(agent_id: str, row: StatRow, authorization: str = Header(default="")):
    auth(authorization)
    if agent_id not in AGENTS:
        raise HTTPException(status_code=404, detail="unknown agent")
    a = AGENTS[agent_id]
    a["last_seen"] = time.time()
    if ACTIVE and row.job_id == ACTIVE["id"]:
        a["working"] = True
        rows = STATS.setdefault(agent_id, [])
        rows.append(row)
        if len(rows) > 7200:
            del rows[:3600]
    return {"ok": True}


@app.post("/api/done/{agent_id}")
def done(agent_id: str, payload: dict, authorization: str = Header(default="")):
    auth(authorization)
    if agent_id in AGENTS:
        AGENTS[agent_id]["last_seen"] = time.time()
        AGENTS[agent_id]["working"] = False
        if ACTIVE and payload.get("job_id") == ACTIVE["id"]:
            ACTIVE["done"].add(agent_id)
    return {"ok": True}


@app.post("/api/jobs")
def create_job(job: JobIn, authorization: str = Header(default=""), token: str = Query(default="")):
    auth(authorization, token)
    global ACTIVE, STATS
    if ACTIVE and time.time() - ACTIVE["created"] > ACTIVE["duration_s"] + 5:
        finalize()
    if ACTIVE:
        raise HTTPException(status_code=409, detail="задача уже идёт — сначала остановите её")
    if not job.url.lower().startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="разрешены только http/https URL")
    ACTIVE = {
        "id": uuid.uuid4().hex[:8], "url": job.url, "method": job.method.upper(),
        "headers": job.headers, "body": job.body, "concurrency": job.concurrency,
        "duration_s": job.duration_s, "timeout_s": job.timeout_s,
        "rampup_s": max(0.0, float(job.rampup_s)),
        "agents": set(job.agents or []), "created": time.time(),
        "done": set(),
    }
    STATS = {}
    return active_view()


@app.post("/api/stop")
def stop(authorization: str = Header(default=""), token: str = Query(default="")):
    auth(authorization, token)
    finalize()
    return {"ok": True}


def finalize():
    global ACTIVE, HISTORY, STATS
    if ACTIVE is None:
        return
    job = ACTIVE
    rows = [r for agent_rows in STATS.values() for r in agent_rows if r.job_id == job["id"]]
    n = sum(r.ok + r.err for r in rows) or 1

    def w(field):
        return round(sum(getattr(r, field) * (r.ok + r.err) for r in rows) / n, 1)

    HISTORY.insert(0, {
        "id": job["id"], "url": job["url"], "method": job["method"],
        "started": job["created"], "finished": time.time(),
        "concurrency": job["concurrency"], "duration_s": job["duration_s"],
        "rampup_s": job["rampup_s"], "timeout_s": job["timeout_s"],
        "body": job["body"], "headers": job["headers"],
        "ok": sum(r.ok for r in rows), "err": sum(r.err for r in rows),
        "p50": w("p50"), "p95": w("p95"), "p99": w("p99"), "avg": w("avg"),
    })
    del HISTORY[20:]
    ACTIVE = None
    STATS = {}


@app.get("/api/state")
def state(token: str = Query(default="")):
    auth(token=token)
    if ACTIVE and time.time() - ACTIVE["created"] > ACTIVE["duration_s"] + 5:
        finalize()
    now = time.time()
    agents = []
    for aid, a in AGENTS.items():
        rows = [r for r in STATS.get(aid, []) if r.ts >= now - 10]
        ok10 = sum(r.ok for r in rows)
        err10 = sum(r.err for r in rows)
        span = max(1.0, now - min((r.ts for r in rows), default=now)) if rows else 1.0
        last = rows[-1] if rows else None
        agents.append({
            "id": aid, "name": a["name"], "host": a["host"], "os": a["os"],
            "working": a["working"], "last_seen": a["last_seen"],
            "rps": round((ok10 + err10) / span, 2),
            "ok10": ok10, "err10": err10,
            "p50": last.p50 if last else None,
            "p95": last.p95 if last else None,
            "p99": last.p99 if last else None,
        })
    job = None
    if ACTIVE:
        sel = ACTIVE["agents"]
        exp = len(AGENTS) if not sel else sum(1 for aid in AGENTS if aid in sel)
        job = {**active_view(), "expected_agents": exp,
               "elapsed": round(time.time() - ACTIVE["created"], 1)}
    return {"agents": agents, "job": job, "history": HISTORY}


@app.get("/api/series")
def series(token: str = Query(default="")):
    """Посекундная агрегированная статистика активной задачи (для графиков)."""
    auth(token=token)
    if not ACTIVE:
        return {"series": [], "job": None}
    jid = ACTIVE["id"]
    # по секундам: ok/err суммируются, перцентили усредняются с весом = числу запросов
    buckets = {}
    for rows in STATS.values():
        for r in rows:
            if r.job_id != jid:
                continue
            b = buckets.setdefault(r.ts, {"ok": 0, "err": 0, "n": 0,
                                          "p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0})
            w = r.ok + r.err
            b["ok"] += r.ok
            b["err"] += r.err
            if w:
                b["n"] += w
                b["p50"] += r.p50 * w
                b["p95"] += r.p95 * w
                b["p99"] += r.p99 * w
                b["avg"] += r.avg * w
    out = []
    for ts in sorted(buckets):
        b = buckets[ts]
        n = b["n"] or 1
        out.append({
            "ts": ts, "ok": b["ok"], "err": b["err"],
            "p50": round(b["p50"] / n, 1), "p95": round(b["p95"] / n, 1),
            "p99": round(b["p99"] / n, 1), "avg": round(b["avg"] / n, 1),
        })
    if len(out) > 3600:  # длинный тест — отдаём последний час
        out = out[-3600:]
    return {"series": out, "job": {"id": jid, "started": ACTIVE["created"]}}


# ---------------------------------------------------------------------------
# Пресеты конфигураций задач: сохранить окно запуска под именем и загрузить
# его одним выбором. Хранятся в presets.json рядом с controller.py, поэтому
# переживают перезапуск контроллера и доступны с любого браузера.
# ---------------------------------------------------------------------------

class PresetIn(BaseModel):
    name: str
    url: str
    method: str = "GET"
    concurrency: int = 10
    duration_s: float = 30.0
    timeout_s: float = 10.0
    rampup_s: float = 0.0
    body: Optional[str] = None
    headers: dict = {}


def _preset_check(p: PresetIn) -> None:
    """Пресет — просто шаблон; валидируем имя и URL, остальное не строгое."""
    if not p.name.strip():
        raise HTTPException(status_code=400, detail="задайте имя пресета")
    if len(p.name) > 64:
        raise HTTPException(status_code=400, detail="имя пресета слишком длинное (макс. 64)")
    if not p.url.lower().startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="разрешены только http/https URL")


@app.get("/api/presets")
def presets_list(authorization: str = Header(default=""), token: str = Query(default="")):
    auth(authorization, token)
    return {"presets": PRESETS}


@app.post("/api/presets")
def presets_add(p: PresetIn, authorization: str = Header(default=""), token: str = Query(default="")):
    auth(authorization, token)
    _preset_check(p)
    item = {"id": uuid.uuid4().hex[:8], **p.model_dump()}
    with _presets_lock:
        PRESETS.insert(0, item)
        del PRESETS[50:]  # пресетов не может стать слишком много
        _presets_save()
    return {"presets": PRESETS}


@app.delete("/api/presets/{pid}")
def presets_del(pid: str, authorization: str = Header(default=""), token: str = Query(default="")):
    auth(authorization, token)
    with _presets_lock:
        PRESETS[:] = [x for x in PRESETS if x["id"] != pid]
        _presets_save()
    return {"presets": PRESETS}


# ---------------------------------------------------------------------------
# Быстрое подключение агентов: контроллер сам раздаёт agent.py и генерирует
# инсталляторы с уже вшитыми адресом контроллера и токеном. На новой машине
# достаточно одной команды из дашборда — никаких ручных шагов.
# ---------------------------------------------------------------------------

SAFE_RE = re.compile(r"^[A-Za-z0-9._~+-]*$")  # для подстановки в sh/ps1/xml
AGENT_FILE = Path(__file__).resolve().parent / "agent.py"

INSTALL_SH = r"""#!/bin/sh
# LoadSwarm agent — автоустановка (macOS / Linux). Сгенерировано контроллером.
# @HEAD@
set -eu
@TRAP@

CTRL="@CONTROLLER@"
TOKEN="@TOKEN@"
BASE="$HOME/.loadswarm"

say(){ printf '%s\n' "$*"; }
die(){ printf 'ОШИБКА: %s\n' "$*" >&2; exit 1; }

command -v curl >/dev/null 2>&1 || die "нужен curl"
command -v python3 >/dev/null 2>&1 || die "python3 не найден — установите Python 3.9+ и повторите"

NAME="@NAME@"
[ -n "$NAME" ] || NAME=$(hostname -s 2>/dev/null || hostname)

mkdir -p "$BASE"
curl -fsSL "$CTRL/agent.py?token=$TOKEN" -o "$BASE/agent.py" || die "не удалось скачать agent.py с $CTRL"

if [ ! -x "$BASE/venv/bin/python" ]; then
  python3 -m venv "$BASE/venv" || die "не удалось создать venv (Debian/Ubuntu: sudo apt install python3-venv)"
fi
"$BASE/venv/bin/pip" install --quiet --disable-pip-version-check aiohttp || die "pip install aiohttp не удался"

cat > "$BASE/run.sh" <<EOF
#!/bin/sh
exec "$BASE/venv/bin/python" "$BASE/agent.py" --controller "$CTRL" --token "$TOKEN" --name "$NAME" >> "$BASE/agent.log" 2>&1
EOF
chmod +x "$BASE/run.sh"

case "$(uname -s)" in
  Darwin)
    PLIST="$HOME/Library/LaunchAgents/com.loadswarm.agent.plist"
    mkdir -p "$HOME/Library/LaunchAgents"
    launchctl unload "$PLIST" 2>/dev/null || true
    AGRN="$NAME"
    cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.loadswarm.agent</string>
  <key>ProgramArguments</key>
  <array>
    <string>$BASE/venv/bin/python</string>
    <string>$BASE/agent.py</string>
    <string>--controller</string><string>$CTRL</string>
    <string>--token</string><string>$TOKEN</string>
    <string>--name</string><string>$AGRN</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$BASE/agent.log</string>
  <key>StandardErrorPath</key><string>$BASE/agent.err</string>
</dict>
</plist>
EOF
    launchctl load "$PLIST"
    say "macOS: агент установлен (launchd, автозапуск при входе)."
    ;;
  Linux)
    if command -v systemctl >/dev/null 2>&1 && systemctl --user >/dev/null 2>&1; then
      SD="$HOME/.config/systemd/user"
      mkdir -p "$SD"
      cat > "$SD/loadswarm-agent.service" <<EOF
[Unit]
Description=LoadSwarm agent
After=network-online.target

[Service]
ExecStart=$BASE/venv/bin/python $BASE/agent.py --controller $CTRL --token $TOKEN
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF
      systemctl --user daemon-reload
      systemctl --user enable --now loadswarm-agent.service
      sudo -n loginctl enable-linger "$USER" 2>/dev/null || \
        say "Подсказка: старт службы при загрузке без входа:  sudo loginctl enable-linger $USER"
      say "Linux: агент установлен (systemd user-служба)."
    else
      (crontab -l 2>/dev/null | grep -v loadswarm-agent; echo "@reboot $BASE/run.sh") | crontab -
      nohup "$BASE/run.sh" >/dev/null 2>&1 &
      say "Linux: агент добавлен в cron (@reboot) и запущен сейчас."
    fi
    ;;
  *)
    nohup "$BASE/run.sh" >/dev/null 2>&1 &
    say "ОС не распознана ($(uname -s)) — агент просто запущен."
    ;;
esac

sleep 2
tail -n 3 "$BASE/agent.log" 2>/dev/null || true
say "Готово. Проверьте дашборд: $CTRL/"
"""

# ASCII-only: Windows PowerShell 5.1 плохо дружит с UTF-8 в iex-пайплайнах
INSTALL_PS1 = r"""# LoadSwarm agent installer for Windows (PowerShell 5.1+).
# Generated by controller @CONTROLLER@
# One line (PowerShell or cmd):
#   powershell -NoProfile -ExecutionPolicy Bypass -Command "irm '@URL@' | iex"
$ErrorActionPreference = "Stop"
$controller = "@CONTROLLER@"
$token      = "@TOKEN@"
$name       = "@NAME@"
if (-not $name) { $name = $env:COMPUTERNAME }

$dir = "$env:USERPROFILE\loadswarm"
New-Item -ItemType Directory -Force $dir | Out-Null

Invoke-WebRequest -UseBasicParsing "$controller/agent.py?token=$token" -OutFile "$dir\agent.py"

python -m pip install --quiet --disable-pip-version-check aiohttp 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "ERROR: pip install aiohttp failed. Install Python from python.org (check 'Add to PATH'), then retry."
  exit 1
}

@"
@echo off
python "$dir\agent.py" --controller "$controller" --token "$token" --name "$name" >> "$dir\agent.log" 2>&1
"@ | Set-Content "$dir\run.cmd" -Encoding ASCII

$elevated = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if ($elevated) {
  schtasks /Create /TN "LoadSwarmAgent" /SC ONSTART /RU SYSTEM /TR "$dir\run.cmd" /F | Out-Null
  schtasks /Run /TN "LoadSwarmAgent" | Out-Null
  Write-Host "Autostart: scheduled task 'LoadSwarmAgent' (ONSTART), started now."
} else {
  $ws = New-Object -ComObject WScript.Shell
  $sc = $ws.CreateShortcut("$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup\LoadSwarm Agent.lnk")
  $sc.TargetPath = "$dir\run.cmd"
  $sc.Save()
  Start-Process "$dir\run.cmd" -WindowStyle Hidden
  Write-Host "Autostart: shortcut created in the Startup folder, agent started now."
}

Start-Sleep 2
if (Test-Path "$dir\agent.log") { Get-Content "$dir\agent.log" -Tail 3 }
Write-Host "Done. The agent should appear on the dashboard: $controller/"
"""


def _install_common(token: str, name: str, controller: str):
    """Валидация и нормализация параметров инсталлятора."""
    if not SAFE_RE.match(token):
        raise HTTPException(status_code=400, detail="недопустимые символы в token")
    if not SAFE_RE.match(name):
        raise HTTPException(status_code=400, detail="недопустимые символы в name")
    tok = token or TOKEN or "none"
    return tok, name, controller.rstrip("/")


@app.get("/agent.py")
def agent_py(token: str = Query(default="")):
    """Раздаём agent.py, чтобы новой машине не нужен был репозиторий."""
    auth(token=token)
    try:
        return Response(AGENT_FILE.read_bytes(), media_type="text/x-python; charset=utf-8")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="agent.py не найден рядом с controller.py")


@app.get("/install.sh")
def install_sh(request: Request, token: str = Query(default=""),
               name: str = Query(default=""), controller: str = Query(default="")):
    """POSIX sh-инсталлятор агента для macOS / Linux (адрес и токен вшиты)."""
    auth(token=token)
    tok, name, ctrl = _install_common(token, name, controller or str(request.base_url))
    script = (INSTALL_SH.replace("@HEAD@", "Одна команда:  curl -fsSL \"<адрес этой установки>\" | sh")
                        .replace("@TRAP@", "")
                        .replace("@CONTROLLER@", ctrl)
                        .replace("@TOKEN@", tok)
                        .replace("@NAME@", name))
    return Response(script, media_type="text/x-shellscript; charset=utf-8")


@app.get("/install.ps1")
def install_ps1(request: Request, token: str = Query(default=""),
                name: str = Query(default=""), controller: str = Query(default="")):
    """PowerShell-инсталлятор агента для Windows (адрес и токен вшиты)."""
    auth(token=token)
    tok, name, ctrl = _install_common(token, name, controller or str(request.base_url))
    script = (INSTALL_PS1.replace("@CONTROLLER@", ctrl)
                         .replace("@TOKEN@", tok)
                         .replace("@NAME@", name)
                         .replace("@URL@", f"{ctrl}/install.ps1?token={tok}"))
    return Response(script, media_type="text/plain; charset=utf-8")


# ---------------------------------------------------------------------------
# Файлы подключения: тот же инсталлятор, но в виде файла, который скачивают
# из панели «Добавить агента» и открывают двойным кликом на своей машине.
# macOS: .command открывается Terminal; Windows: .bat — cmd, из него iex-пайплайн.
# Окно не закрывается само (pause), чтобы результат был виден пользователю.
# ---------------------------------------------------------------------------

# Пауза в конце join.command: Trap срабатывает и при успехе, и после die() —
# в curl-пайплайне его быть не должно (read съел бы остаток скрипта из stdin).
TRAP_PAUSE = ("finish(){ printf '\\n=== Готово — окно можно закрыть (Enter) ===\\n'; "
              "read _ >/dev/null 2>&1 || true; }\n"
              "trap finish EXIT")

JOIN_BAT = """@echo off
rem LoadSwarm agent installer for Windows - generated by controller @CONTROLLER@
rem Download this file on YOUR OWN machine and double-click it.
rem It installs the LoadSwarm agent (aiohttp) and registers autostart.
powershell -NoProfile -ExecutionPolicy Bypass -Command "irm '@CONTROLLER@/install.ps1?token=@TOKEN@' | iex"
echo.
echo === Done - the agent should appear on the dashboard shortly ===
pause >nul
"""


@app.get("/join.command")
def join_command(request: Request, token: str = Query(default=""),
                 name: str = Query(default=""), controller: str = Query(default="")):
    """macOS: инсталлятор-файл — скачали из дашборда, открыли, машина в рое."""
    auth(token=token)
    tok, name, ctrl = _install_common(token, name, controller or str(request.base_url))
    script = (INSTALL_SH.replace("@HEAD@", "macOS: скачайте этот файл и откройте двойным кликом — "
                                         "Terminal сам всё установит и не закроет окно.")
                        .replace("@TRAP@", TRAP_PAUSE)
                        .replace("@CONTROLLER@", ctrl)
                        .replace("@TOKEN@", tok)
                        .replace("@NAME@", name))
    return Response(script, media_type="text/x-shellscript; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="join.command"'})


@app.get("/join.bat")
def join_bat(request: Request, token: str = Query(default=""),
             controller: str = Query(default="")):
    """Windows: инсталлятор-файл — двойной клик, дальше всё делает install.ps1.

    ASCII-only и CRLF: cmd.exe некорректно читает UTF-8/LF-батники.
    Имя агента — COMPUTERNAME (своё имя задают командой из панели).
    """
    auth(token=token)
    tok, _name, ctrl = _install_common(token, "", controller or str(request.base_url))
    script = (JOIN_BAT.replace("@CONTROLLER@", ctrl)
                      .replace("@TOKEN@", tok)
                      .replace("\n", "\r\n"))
    return Response(script, media_type="text/plain; charset=ascii",
                    headers={"Content-Disposition": 'attachment; filename="join.bat"'})


DASHBOARD = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LoadSwarm</title>
<style>
 body{font-family:system-ui,sans-serif;max-width:980px;margin:20px auto;padding:0 12px;background:#0e1420;color:#dbe4f0}
 h1{font-size:22px;margin:12px 0}
 h2{font-size:16px;margin:20px 0 4px}
 .muted{color:#7d8aa5;font-weight:400;font-size:14px}
 input,select,textarea,button{background:#1a2233;color:#dbe4f0;border:1px solid #35415e;border-radius:6px;padding:6px 8px;font-size:14px}
 textarea{width:100%;font-family:ui-monospace,monospace;resize:vertical}
 button{cursor:pointer}button:disabled{opacity:.45}
 table{border-collapse:collapse;width:100%;margin-top:8px}
 th,td{border-bottom:1px solid #232c42;padding:6px 10px;font-size:14px;text-align:right;white-space:nowrap}
 th{color:#7d8aa5;font-weight:600}
 td:first-child,th:first-child{text-align:left}
 .ok{color:#4ade80}.err{color:#f87171}
 form{background:#151d2e;border:1px solid #232c42;border-radius:10px;padding:14px;margin-top:8px}
 .row{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;align-items:flex-end}
 .row label{display:flex;flex-direction:column;gap:4px;font-size:12px;color:#7d8aa5}
 #msg{font-size:13px}
 .charts{display:flex;gap:10px;flex-wrap:wrap;margin-top:8px}
 .chartcard{flex:1;min-width:320px;background:#151d2e;border:1px solid #232c42;border-radius:10px;padding:10px}
 .ctitle{font-size:12px;color:#7d8aa5;margin-bottom:6px}
 canvas{width:100%;display:block}
 .cmd{display:flex;gap:8px;align-items:center;background:#151d2e;border:1px solid #232c42;border-radius:8px;padding:8px 10px;margin-top:6px}
 .cmd b{white-space:nowrap}
 .cmd code{font-family:ui-monospace,monospace;font-size:12.5px;word-break:break-all;color:#a5c4f0;flex:1}
 .copybtn{white-space:nowrap;font-size:12px}
 a{color:#a5c4f0}
 .overlay{position:fixed;inset:0;background:rgba(4,8,16,.72);display:none;z-index:20;overflow:auto}
 .overlay.open{display:block}
 .modal{background:#151d2e;border:1px solid #35415e;border-radius:12px;padding:16px 18px;max-width:680px;margin:46px auto}
 .modal h2{margin-top:0}
 .chk{display:inline-flex;align-items:center;gap:5px;font-size:13px;padding:3px 8px;background:#1a2233;border:1px solid #35415e;border-radius:6px}
 .est{font-size:13px;color:#a5c4f0}
 #merr{color:#f87171;font-size:13px}
</style>
</head>
<body>
<h1>🐝 LoadSwarm <span class="muted">— роевое нагрузочное тестирование своих серверов</span></h1>
<div class="row">
 <input id="token" type="password" placeholder="token" style="width:200px" oninput="renderAddAgent()">
 <button onclick="refresh()">обновить</button>
 <span id="msg" class="muted"></span>
</div>
<div id="jobbox"></div>

<div class="row">
 <button id="launchbtn" onclick="openModal()">▶ Запустить нагрузку</button>
 <button onclick="stopJob()">■ остановить</button>
 <span id="estline" class="muted"></span>
</div>
<div id="jobbox"></div>

<div id="overlay" class="overlay">
 <div class="modal">
  <h2>▶ Новая нагрузочная задача <span class="muted">— только свой сервер</span></h2>
  <div class="row">
   <label style="flex:1">URL своего сервера<input id="url" size="40" placeholder="http://10.0.0.2:8000/api/health" required></label>
   <label>метод<select id="method"><option>GET</option><option>POST</option><option>PUT</option><option>DELETE</option><option>PATCH</option></select></label>
  </div>
  <div class="row" style="align-items:center">
   <select id="presets" onchange="loadPreset(this.value)" style="max-width:210px"><option value="">— пресет —</option></select>
   <button type="button" class="copybtn" onclick="delPreset()" title="удалить выбранный пресет">×</button>
   <input id="pname" placeholder="имя пресета" style="width:150px" maxlength="64">
   <button type="button" class="copybtn" onclick="savePreset()" title="сохранить текущую конфигурацию как пресет">💾 сохранить</button>
  </div>
  <div class="row">
   <label>concurrency / агент<input id="conc" type="number" value="10" min="1" style="width:80px"></label>
   <label>разгон ramp-up, с<input id="ramp" type="number" value="0" min="0" style="width:70px"></label>
   <label>длительность, с<input id="dur" type="number" value="30" min="1" style="width:70px"></label>
   <label>таймаут, с<input id="tmo" type="number" value="10" min="1" style="width:70px"></label>
  </div>
  <div class="row">
   <label style="flex:1">тело запроса (опционально)<textarea id="body" rows="2" placeholder='{"key": "value"}'></textarea></label>
   <label style="flex:1">заголовки, JSON (опционально)<textarea id="hdrs" rows="2" placeholder='{"Content-Type": "application/json"}'></textarea></label>
  </div>
  <div class="row" style="align-items:center">
   <span class="muted">агенты:</span>
   <span id="agsel"></span>
   <button type="button" class="copybtn" onclick="checkAll(true)">все</button>
   <button type="button" class="copybtn" onclick="checkAll(false)">снять</button>
   <span class="muted">ничего не отмечено = участвуют все</span>
  </div>
  <div class="row"><span id="est" class="est"></span></div>
  <div class="row">
   <button onclick="launch()">▶ запустить</button>
   <button onclick="closeModal()">отмена</button>
   <span id="merr"></span>
  </div>
 </div>
</div>

<h2>Графики <span class="muted" id="chartinfo"></span></h2>
<div class="charts">
 <div class="chartcard"><div class="ctitle">RPS — <span class="ok">ok</span> / <span class="err">err</span></div><canvas id="c1" data-h="170"></canvas></div>
 <div class="chartcard"><div class="ctitle">Латентность, мс — avg / p95 / p99</div><canvas id="c2" data-h="170"></canvas></div>
</div>

<h2>Добавить агента <span class="muted">— на свою машину, одна команда</span></h2>
<div id="addagent"></div>

<h2>Агенты</h2>
<table><thead><tr><th>имя</th><th>хост</th><th>ОС</th><th>статус</th><th>RPS</th><th>ok/10с</th><th>err/10с</th><th>p50</th><th>p95</th><th>p99</th><th>видел</th></tr></thead>
<tbody id="agents"></tbody></table>

<h2>Завершённые задачи</h2>
<table><thead><tr><th>когда</th><th>метод</th><th>URL</th><th>ok</th><th>err</th><th>avg</th><th>p95</th><th>p99</th><th></th></tr></thead>
<tbody id="hist"></tbody></table>

<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
function tok(){const v=$("token").value.trim();if(v)localStorage.loadswarm_token=v;return v||localStorage.loadswarm_token||"";}
function ago(ts){const d=Math.max(0,Date.now()/1000-ts);return d<90?Math.round(d)+"s ago":d<5400?Math.round(d/60)+"m ago":Math.round(d/3600)+"h ago";}
async function api(path,opts={}){
 const sep=path.includes("?")?"&":"?";
 const r=await fetch(path+sep+"token="+encodeURIComponent(tok()),opts);
 if(!r.ok){throw new Error(r.status+" "+(await r.text()).slice(0,120));}
 return r.status===204?null:r.json();
}
async function refresh(){
 try{
  const s=await api("/api/state");
  if($("msg").textContent.startsWith("⚠"))$("msg").textContent="";
  AGENTS=s.agents||[];
  $("estline").textContent=AGENTS.length?("агентов на связи: "+AGENTS.length):"агентов нет — панель «Добавить агента» ниже";
  renderJob(s.job);renderAgents(AGENTS);renderHist(s.history||[]);
 }catch(e){$("msg").textContent="⚠ "+e.message;}
 try{const sr=await api("/api/series");SERIES=sr.series||[];}catch(e){SERIES=[];}
 try{const pr=await api("/api/presets");renderPresets(pr.presets||[]);}catch(e){}
 renderCharts();
}
function renderJob(job){
 $("jobbox").innerHTML = !job
  ? "<p class=muted>Нет активной задачи. Агенты в режиме ожидания, опрашивают контроллер.</p>"
  : `<p>▶ задача <b>${esc(job.id)}</b>: <b>${esc(job.method)} ${esc(job.url)}</b> — concurrency ${job.concurrency} × ${job.expected_agents} агентов ≈ <b>${job.concurrency*job.expected_agents}</b> виртуальных пользователей · разгон ${job.rampup_s||0}с · ${Math.round(job.duration_s)}с · прошло ${Math.round(job.elapsed)}с</p>`;
 $("chartinfo").textContent = job?("задача "+job.id):"нет активной задачи";
 $("launchbtn").disabled = !!job;
}
function renderAgents(list){
 const tb=$("agents");tb.innerHTML="";
 let rps=0,ok=0,err=0;
 for(const a of list){
  rps+=a.rps;ok+=a.ok10;err+=a.err10;
  const tr=document.createElement("tr");
  tr.innerHTML=`<td>${esc(a.name)}</td><td>${esc(a.host||"—")}</td><td>${esc(a.os||"—")}</td>`+
   `<td class="${a.working?"ok":"muted"}">${a.working?"работает":"простой"}</td><td>${a.rps}</td>`+
   `<td class=ok>${a.ok10}</td><td class="${a.err10?"err":""}">${a.err10}</td>`+
   `<td>${a.p50??"—"}</td><td>${a.p95??"—"}</td><td>${a.p99??"—"}</td><td class=muted>${ago(a.last_seen)}</td>`;
  tb.appendChild(tr);
 }
 const tr=document.createElement("tr");
 tr.innerHTML=`<td><b>Итого</b></td><td colspan=3></td><td><b>${rps.toFixed(2)}</b></td><td class=ok><b>${ok}</b></td><td class="${err?"err":""}"><b>${err}</b></td><td colspan=4 class=muted>за последние 10с</td>`;
 tb.appendChild(tr);
}
function renderHist(list){
 HIST=list||[];
 const tb=$("hist");tb.innerHTML="";
 if(!list.length){tb.innerHTML=`<tr><td colspan=9 class=muted>пусто</td></tr>`;return;}
 for(const j of list){
  const tr=document.createElement("tr");
  const url=j.url.length>55?j.url.slice(0,52)+"…":j.url;
  tr.innerHTML=`<td class=muted>${ago(j.finished)}</td><td>${esc(j.method)}</td>`+
   `<td>${esc(url)}</td><td class=ok>${j.ok}</td><td class="${j.err?"err":""}">${j.err}</td>`+
   `<td>${j.avg} ms</td><td>${j.p95} ms</td><td>${j.p99} ms</td>`+
   `<td><button class=copybtn onclick="repeatJob(${list.indexOf(j)})">повторить</button></td>`;
  tb.appendChild(tr);
 }
}
let SERIES=[];
let AGENTS=[];
let PRESETS=[];
let HIST=[];
async function cp(b){
 try{await navigator.clipboard.writeText(b.dataset.cmd);
  b.textContent="скопировано ✓";setTimeout(()=>b.textContent="копировать",1500);}
 catch(e){b.textContent="⚠ вручную";}
}
function renderAddAgent(){
 const t=tok(),base=location.origin,box=$("addagent");
 if(!t){box.innerHTML="<p class=muted>Введите токен сверху — готовые команды появятся здесь.</p>";return;}
 const u=encodeURIComponent(t);
 const mac=`curl -fsSL "${base}/install.sh?token=${u}" | sh`;
 const win=`powershell -NoProfile -ExecutionPolicy Bypass -Command "irm '${base}/install.ps1?token=${u}' | iex"`;
 const jc=`${base}/join.command?token=${u}`,jb=`${base}/join.bat?token=${u}`;
 box.innerHTML=
  `<div class=cmd><b>macOS</b><a href="${esc(jc)}">скачать join.command</a><span class=muted>открыть двойным кликом — Terminal сам всё установит, окно не закроется</span></div>`+
  `<div class=cmd><b>Windows</b><a href="${esc(jb)}">скачать join.bat</a><span class=muted>открыть двойным кликом</span></div>`+
  `<div class=cmd><b>macOS / Linux</b><code>${esc(mac)}</code><button class=copybtn data-cmd="${esc(mac)}" onclick="cp(this)">копировать</button></div>`+
  `<div class=cmd><b>Windows</b><code>${esc(win)}</code><button class=copybtn data-cmd="${esc(win)}" onclick="cp(this)">копировать</button></div>`+
  `<p class=muted>Файлы и команды содержат токен — используйте их только на своих машинах. Инсталлятор сам скачает агента у контроллера, поставит aiohttp, зарегистрирует автозапуск и запустит его; машина появится в таблице ниже через ~10 с.</p>`;
}
function drawChart(cv,keys,colors){
 const dpr=window.devicePixelRatio||1;
 const w=Math.max(200,cv.parentElement.clientWidth-20);
 const h=+cv.dataset.h||170;  // data-h: attr "height" мутируется установкой cv.height
 cv.width=w*dpr;cv.height=h*dpr;
 const g=cv.getContext("2d");g.setTransform(dpr,0,0,dpr,0,0);g.clearRect(0,0,w,h);
 if(!SERIES.length){g.fillStyle="#7d8aa5";g.font="12px system-ui";g.fillText("нет активной задачи — линии появятся во время теста",10,24);return;}
 const t0=SERIES[0].ts,t1=Math.max(t0+1,SERIES[SERIES.length-1].ts);
 let ymax=1;for(const p of SERIES)for(const k of keys)if(p[k]>ymax)ymax=p[k];
 ymax*=1.15;
 const L=48,R=8,T=8,B=18,W=w-L-R,H=h-T-B;
 const prevs=keys.map(()=>null);
 g.strokeStyle="#232c42";g.fillStyle="#7d8aa5";g.font="10px ui-monospace";g.lineWidth=1;
 for(let i=0;i<=3;i++){
  const y=T+H-H*i/3;
  g.beginPath();g.moveTo(L,y);g.lineTo(w-R,y);g.stroke();
  const v=ymax*i/3;g.fillText(v>=1000?(v/1000).toFixed(1)+"k":Math.round(v),2,y+3);
 }
 for(const p of SERIES){
  const t=(p.ts-t0)/(t1-t0);
  keys.forEach((k,ki)=>{
   const X=L+t*W,Y=T+H-(p[k]||0)/ymax*H;
   g.strokeStyle=colors[ki];g.lineWidth=1.5;
   if(prevs[ki]){g.beginPath();g.moveTo(prevs[ki][0],prevs[ki][1]);g.lineTo(X,Y);g.stroke();}
   prevs[ki]=[X,Y];
  });
 }
 g.fillStyle="#7d8aa5";g.font="10px system-ui";
 g.fillText(Math.round(t1-t0)+"с",w-R-20,h-5);
}
function renderCharts(){
 const c1=$("c1"),c2=$("c2");
 if(!c1||!c2)return;
 drawChart(c1,["ok","err"],["#4ade80","#f87171"]);
 drawChart(c2,["avg","p95","p99"],["#60a5fa","#fbbf24","#f87171"]);
}
function checkedIds(){return [...document.querySelectorAll("#agsel input:checked")].map(c=>c.value);}
function renderAgsel(){
 const box=$("agsel");
 if(!AGENTS.length){box.innerHTML="<span class=muted>агентов нет — подключите хотя бы одного ниже</span>";return;}
 box.innerHTML=AGENTS.map(a=>`<label class=chk><input type=checkbox value="${esc(a.id)}"> ${esc(a.name)}</label>`).join(" ");
}
function checkAll(v){document.querySelectorAll("#agsel input[type=checkbox]").forEach(c=>c.checked=v);updateEst();}
function updateEst(){
 const n=checkedIds().length||AGENTS.length,c=+$("conc").value||0,r=+$("ramp").value||0;
 $("est").textContent=`итого: ${n} агент(ов) × ${c} = ${n*c} виртуальных пользователей`+(r>0?`, разгон ${r} с`:"");
}
function openModal(){
 if($("launchbtn").disabled){$("msg").textContent="задача уже идёт — сначала остановите её";return;}
 $("merr").textContent="";renderAgsel();updateEst();
 $("overlay").classList.add("open");$("url").focus();
}
function closeModal(){$("overlay").classList.remove("open");}
document.addEventListener("keydown",e=>{if(e.key==="Escape")closeModal();});
function gatherJob(){
 const body={url:$("url").value.trim(),method:$("method").value,
  concurrency:+$("conc").value||10,duration_s:+$("dur").value||30,timeout_s:+$("tmo").value||10,
  rampup_s:+$("ramp").value||0,agents:checkedIds()};
 if(!body.url)throw new Error("укажите URL");
 if($("body").value.trim())body.body=$("body").value;
 if($("hdrs").value.trim())body.headers=JSON.parse($("hdrs").value);
 return body;
}
async function launch(){
 $("merr").textContent="";
 try{
  await api("/api/jobs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(gatherJob())});
  $("msg").textContent="задача создана ✓";closeModal();
 }catch(e){$("merr").textContent="⚠ "+e.message;}
 refresh();
}
function fillJob(j){
 $("url").value=j.url||"";$("method").value=j.method||"GET";
 $("conc").value=j.concurrency??10;$("ramp").value=j.rampup_s||0;
 $("dur").value=j.duration_s??30;$("tmo").value=j.timeout_s??10;
 $("body").value=j.body||"";
 $("hdrs").value=j.headers&&Object.keys(j.headers).length?JSON.stringify(j.headers):"";
 updateEst();
}
function renderPresets(list){
 PRESETS=list||[];
 const sel=$("presets"),cur=sel.value;
 sel.innerHTML=`<option value="">— пресет —</option>`+
  PRESETS.map(p=>`<option value="${esc(p.id)}">${esc(p.name)}</option>`).join("");
 sel.value=PRESETS.some(p=>p.id===cur)?cur:"";
}
function loadPreset(id){const p=PRESETS.find(x=>x.id===id);if(p)fillJob(p);}
async function savePreset(){
 $("merr").textContent="";
 try{
  const name=$("pname").value.trim();
  if(!name)throw new Error("задайте имя пресета");
  const j=gatherJob();delete j.agents;j.name=name;
  const r=await api("/api/presets",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(j)});
  renderPresets(r.presets||[]);
  $("presets").value=PRESETS[0]?PRESETS[0].id:"";
  $("pname").value="";$("msg").textContent="пресет сохранён ✓";
 }catch(e){$("merr").textContent="⚠ "+e.message;}
}
async function delPreset(){
 const id=$("presets").value;if(!id)return;
 if(!confirm("Удалить выбранный пресет?"))return;
 try{const r=await api("/api/presets/"+id,{method:"DELETE"});renderPresets(r.presets||[]);}
 catch(e){$("msg").textContent="⚠ "+e.message;}
}
function repeatJob(i){
 openModal();
 if(!$("overlay").classList.contains("open"))return;
 if(HIST[i])fillJob(HIST[i]);
}
async function stopJob(){try{await api("/api/stop",{method:"POST"});$("msg").textContent="остановлено";}catch(e){$("msg").textContent="⚠ "+e.message;}refresh();}
$("token").value=localStorage.loadswarm_token||"";
renderAddAgent();refresh();setInterval(refresh,2000);
$("conc").addEventListener("input",updateEst);
$("ramp").addEventListener("input",updateEst);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return DASHBOARD


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LoadSwarm controller")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    if not TOKEN:
        print("ПРЕДУПРЕЖДЕНИЕ: LOADSWARM_TOKEN не задан — API открыт. "
              "Обязательно задайте токен, если контроллер доступен из интернета.", flush=True)
    uvicorn.run(app, host=args.host, port=args.port, access_log=False)
