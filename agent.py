#!/usr/bin/env python3
"""LoadSwarm agent — генератор нагрузки для тестирования СВОИХ серверов.

Агент исходящим соединением подключается к контроллеру, забирает задачу,
генерирует HTTP-нагрузку и раз в секунду отправляет статистику.

Запускайте только против серверов, которыми владеете или имеете
письменное разрешение тестировать.
"""
import argparse
import asyncio
import os
import platform
import socket
import sys
import time

try:
    import aiohttp
except ImportError:
    sys.exit("Нет зависимости. Выполните:  pip install aiohttp")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def pctl(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, round(len(s) * p / 100))]


class Agent:
    def __init__(self, controller: str, token: str, name: str):
        self.controller = controller.rstrip("/")
        self.token = token
        self.name = name
        self.agent_id = None
        self.last_done = None

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    async def _call(self, http, method: str, path: str, payload=None):
        """Возвращает (True, data), (True, None) при 204 или (False, причина)."""
        try:
            async with http.request(
                method, self.controller + path, json=payload,
                headers=self._headers(), timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 204:
                    return True, None
                if r.status >= 400:
                    return False, f"HTTP {r.status}: {(await r.text())[:200]}"
                return True, await r.json(content_type=None)
        except Exception as e:
            return False, str(e)

    async def register(self, http) -> bool:
        ok, data = await self._call(http, "POST", "/api/join", {
            "name": self.name,
            "host": socket.gethostname(),
            "os": f"{platform.system()} {platform.release()}",
            "py": platform.python_version(),
        })
        if ok and data and data.get("id"):
            self.agent_id = data["id"]
            log(f"зарегистрирован на контроллере, id={self.agent_id}")
            return True
        log(f"регистрация не удалась: {data}")
        return False

    async def get_job(self, http):
        ok, data = await self._call(http, "GET", f"/api/job/{self.agent_id}")
        return data if ok else "unreachable"

    async def run_job(self, http, job: dict) -> None:
        url = job["url"]
        method = job.get("method", "GET").upper()
        headers = job.get("headers") or {}
        body = job.get("body")
        conc = max(1, int(job.get("concurrency", 10)))
        duration = float(job.get("duration_s", 30))
        rampup = max(0.0, float(job.get("rampup_s", 0)))
        jid = job["id"]
        log(f"задача {jid}: {method} {url} concurrency={conc} rampup={rampup}s "
            f"duration={duration}s")

        bucket = {"ok": 0, "err": 0, "lat": [], "codes": {}}
        totals = {"ok": 0, "err": 0}
        deadline = time.monotonic() + duration
        stop = asyncio.Event()
        # Предохранитель: не превращать недоступный таргет в шторм коннектов.
        # Ядро macOS (rst_rlc) троттлит новые соединения после ~2000+ RST/с,
        # из-за чего после шторма даже живой таргет подвисает на секунды.
        fail_ts = []  # штампы недавних транспортных ошибок (monotonic)
        breaker_logged = False

        def breaker_engaged() -> bool:
            now = time.monotonic()
            while fail_ts and now - fail_ts[0] > 1.0:
                fail_ts.pop(0)
            return len(fail_ts) >= 100  # не больше 100 неудачных коннектов в секунду

        async def worker(start_delay: float = 0.0) -> None:
            nonlocal breaker_logged
            if start_delay > 0:
                # ramp-up: воркеры входят в работу волной; последний — не позже,
                # чем за 1 с до конца задачи, чтобы он успел сделать запросы
                await asyncio.sleep(min(start_delay, max(0.0, duration - 1)))
            timeout = aiohttp.ClientTimeout(total=float(job.get("timeout_s", 10)))
            while not stop.is_set() and time.monotonic() < deadline:
                if breaker_engaged():
                    if not breaker_logged:
                        log("предохранитель: таргет отвечает ошибками, снижаю темп")
                        breaker_logged = True
                    await asyncio.sleep(0.25)
                    continue
                t0 = time.perf_counter()
                status = 0
                try:
                    async with http.request(method, url, headers=headers,
                                            data=body, timeout=timeout) as r:
                        await r.read()
                        status = r.status
                except Exception:
                    status = 0
                    fail_ts.append(time.monotonic())
                lat = time.perf_counter() - t0
                if 200 <= status < 400:
                    bucket["ok"] += 1
                    totals["ok"] += 1
                else:
                    bucket["err"] += 1
                    totals["err"] += 1
                bucket["codes"][status] = bucket["codes"].get(status, 0) + 1
                bucket["lat"].append(lat)

        async def reporter() -> None:
            while not stop.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(1.0)
                ok, current = await self._call(http, "GET", f"/api/job/{self.agent_id}")
                if ok and (current is None or current.get("id") != jid):
                    log("задача отменена контроллером")
                    stop.set()
                lat = bucket["lat"]
                codes = dict(bucket["codes"])
                row = {
                    "job_id": jid, "ts": int(time.time()),
                    "ok": bucket["ok"], "err": bucket["err"],
                    "p50": round(pctl(lat, 50) * 1000, 1),
                    "p95": round(pctl(lat, 95) * 1000, 1),
                    "p99": round(pctl(lat, 99) * 1000, 1),
                    "avg": round(sum(lat) / len(lat) * 1000, 1) if lat else 0.0,
                    "codes": codes,
                }
                bucket["ok"] = 0
                bucket["err"] = 0
                bucket["lat"] = []
                bucket["codes"] = {}
                sent, err = await self._call(http, "POST", f"/api/stats/{self.agent_id}", row)
                if not sent:
                    log(f"статистика не отправлена: {err}")

        tasks = [asyncio.create_task(worker(rampup * i / conc)) for i in range(conc)]
        rep = asyncio.create_task(reporter())
        await asyncio.gather(*tasks, rep)
        stop.set()
        # досылаем финальную строку за последнюю неполную секунду
        lat = bucket["lat"]
        if bucket["ok"] or bucket["err"] or lat:
            await self._call(http, "POST", f"/api/stats/{self.agent_id}", {
                "job_id": jid, "ts": int(time.time()),
                "ok": bucket["ok"], "err": bucket["err"],
                "p50": round(pctl(lat, 50) * 1000, 1),
                "p95": round(pctl(lat, 95) * 1000, 1),
                "p99": round(pctl(lat, 99) * 1000, 1),
                "avg": round(sum(lat) / len(lat) * 1000, 1) if lat else 0.0,
                "codes": dict(bucket["codes"]),
            })
        log(f"задача {jid} завершена: ok={totals['ok']} err={totals['err']}")
        self.last_done = jid
        await self._call(http, "POST", f"/api/done/{self.agent_id}", {"job_id": jid})


async def amain(args) -> None:
    agent = Agent(args.controller, args.token, args.name)
    # limit=0 — не ограничивать пул соединений сверху (по умолчанию в aiohttp 100)
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as http:
        while not await agent.register(http):
            await asyncio.sleep(5)
        misses = 0
        while True:
            job = await agent.get_job(http)
            if job == "unreachable":
                misses += 1
                if misses >= 5:
                    log("контроллер недоступен, перерегистрируюсь")
                    agent.agent_id = None
                    while not await agent.register(http):
                        await asyncio.sleep(5)
                    misses = 0
            elif isinstance(job, dict) and job.get("id") == agent.last_done:
                misses = 0
                await asyncio.sleep(3)
            elif job is None:
                agent.last_done = None
                misses = 0
                await asyncio.sleep(3)
            else:
                misses = 0
                try:
                    await agent.run_job(http, job)
                except Exception as e:
                    log(f"ошибка задачи: {e}")
                    await asyncio.sleep(2)


def parse_args():
    p = argparse.ArgumentParser(description="LoadSwarm agent")
    p.add_argument("--controller", default=os.environ.get("LOADSWARM_CONTROLLER", ""),
                   help="URL контроллера, например http://10.0.0.1:8080")
    p.add_argument("--token", default=os.environ.get("LOADSWARM_TOKEN", ""),
                   help="общий токен (тот же, что на контроллере)")
    p.add_argument("--name", default=socket.gethostname(), help="имя агента")
    args = p.parse_args()
    if not args.controller or not args.token:
        p.error("укажите --controller и --token (или переменные LOADSWARM_CONTROLLER / LOADSWARM_TOKEN)")
    return args


if __name__ == "__main__":
    try:
        asyncio.run(amain(parse_args()))
    except KeyboardInterrupt:
        print("остановлено")
