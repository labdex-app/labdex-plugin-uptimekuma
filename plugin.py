"""Uptime Kuma: monitors from services (plan/apply) and their status, over Kuma's socket.io API."""
from __future__ import annotations

import time

from labdex.plugins.base import Change, PluginError
from labdex.plugins.base import Plugin as Base

MARK = "labdex:"  # description prefix of the monitors labdex manages: labdex:<host>/<service>
STATUS = {0: "down", 1: "up", 2: "pending", 3: "maintenance"}
DEFAULTS = {
    "method": "GET", "maxredirects": 10, "accepted_statuscodes": ["200-299"], "upsideDown": False, "resendInterval": 0,
    "timeout": 48, "httpBodyEncoding": "json", "packetSize": 56, "expiryNotification": True,
    "dns_resolve_server": "1.1.1.1", "dns_resolve_type": "A", "kafkaProducerBrokers": [], "kafkaProducerSaslOptions": {},
    "conditions": [], "rabbitmqNodes": [],
}


class Kuma:
    """A logged-in socket.io session; collects monitorList and heartbeats."""

    TRANSPORTS = None  # default: polling, upgraded to websocket

    def __init__(self, url, username, password, verify=True, log=None):
        try:
            import socketio
        except ImportError as exc:
            raise PluginError("python-socketio isn't installed (pip install 'python-socketio[client]')") from exc
        self.log = log or (lambda m: None)
        self.monitors: dict[int, dict] = {}
        self.beats: dict[int, list] = {}
        self.sio = socketio.Client(reconnection=False, ssl_verify=verify)
        self.sio.on("monitorList", self._monitor_list)
        self.sio.on("heartbeatList", self._heartbeat_list)
        self.sio.on("heartbeat", self._heartbeat)
        self._got_list = False
        try:
            self.sio.connect(url.rstrip("/"), socketio_path="socket.io", wait_timeout=10, transports=self.TRANSPORTS)
        except Exception as exc:  # socketio.exceptions.ConnectionError and friends
            raise PluginError(f"can't connect to Uptime Kuma at {url}: {exc}") from exc
        res = self.call("login", {"username": username, "password": password, "token": ""})
        if not res.get("ok"):
            self.close()
            raise PluginError(f"Uptime Kuma refused the login: {res.get('msg') or res}")
        end = time.monotonic() + 10
        while not self._got_list and time.monotonic() < end:
            time.sleep(0.05)

    def _monitor_list(self, data):
        self.monitors = {int(k): v for k, v in (data or {}).items()}
        self._got_list = True

    def _heartbeat_list(self, monitor_id, data, overwrite=False):
        mid = int(monitor_id)
        self.beats[mid] = list(data or []) if overwrite or mid not in self.beats else self.beats[mid] + list(data or [])

    def _heartbeat(self, data):
        self.beats.setdefault(int(data.get("monitorID", 0)), []).append(data)

    def call(self, event, *args):
        try:
            res = self.sio.call(event, args[0] if len(args) == 1 else args, timeout=15)
        except Exception as exc:
            raise PluginError(f"Uptime Kuma didn't answer {event}: {exc}") from exc
        return res if isinstance(res, dict) else {"ok": bool(res)}

    def status(self, mid: int) -> str:
        beats = self.beats.get(mid) or []
        if not beats:
            return "unknown" if self.monitors.get(mid, {}).get("active", True) else "paused"
        last = max(beats, key=lambda b: str(b.get("time", "")))
        return STATUS.get(int(last.get("status", -1)), "unknown")

    def close(self):
        try:
            self.sio.disconnect()
        except Exception:
            pass


class Plugin(Base):
    def _kuma(self) -> Kuma:
        s = self.settings
        if not s.get("url"):
            raise PluginError("set the URL first")
        return Kuma(s["url"], s.get("username", ""), s.get("password", ""), bool(s.get("verify_tls", True)), self.log)

    def test(self):
        k = self._kuma()
        try:
            managed = sum(1 for m in k.monitors.values() if str(m.get("description") or "").startswith(MARK))
            return f"{len(k.monitors)} monitors, {managed} managed by labdex"
        finally:
            k.close()

    # --------------------------------------------------------------- wanted
    def _wanted(self) -> dict[str, dict]:
        """key (host/service) → monitor fields, for the hosts this instance applies to."""
        from labdex import store
        from labdex.models import Host, db
        s = self.settings
        names = {h["name"] for h in self.ctx.hosts()}
        out = {}
        for h in db.session.scalars(db.select(Host).where(Host.name.in_(names)).order_by(Host.name)):
            for svc in h.services:
                key = f"{h.name}/{svc.name}"
                base = {"name": f"{h.fun or h.name} {svc.name}", "description": f"{MARK}{key}",
                        "interval": int(s.get("interval") or 60), "retryInterval": int(s.get("interval") or 60),
                        "maxretries": int(s.get("retries") or 0)}
                url = store.service_url(h, svc)
                if url:
                    out[key] = base | {"type": "http", "url": url, "ignoreTls": bool(s.get("ignore_tls"))}
                elif s.get("services") == "tcp" and svc.port and (svc.proto or "tcp") == "tcp":
                    addrs = store.service_addresses(h, svc)
                    addr = next((a for a in addrs if ":" not in a), addrs[0] if addrs else "")
                    if addr:
                        out[key] = base | {"type": "port", "hostname": addr, "port": int(svc.port)}
        return out

    @staticmethod
    def _differs(cur: dict, want: dict) -> list[str]:
        return [k for k, v in want.items() if k != "description" and cur.get(k) != v]

    def plan_sync_monitors(self, params):
        k = self._kuma()
        try:
            have = {str(m.get("description"))[len(MARK):]: m for m in k.monitors.values() if str(m.get("description") or "").startswith(MARK)}
        finally:
            k.close()
        want = self._wanted()
        changes = []
        for key, w in sorted(want.items()):
            cur = have.get(key)
            if cur is None:
                changes.append(Change("create", "monitor", w["name"], w.get("url") or f"{w.get('hostname')}:{w.get('port')}", after=w, data={"key": key}))
            else:
                diff = self._differs(cur, w)
                if diff:
                    changes.append(Change("update", "monitor", w["name"], "changes " + ", ".join(diff),
                                          before={d: cur.get(d) for d in diff}, after={d: w[d] for d in diff}, data={"key": key, "id": cur["id"]}))
        for key, cur in sorted(have.items()):
            if key not in want:
                changes.append(Change("delete", "monitor", cur.get("name", key), "the service is gone or has no link", data={"key": key, "id": cur["id"]}))
        return changes

    def apply_sync_monitors(self, changes, params):
        notif = {str(n): True for n in self.settings.get("notifications") or []}
        want = self._wanted()
        k = self._kuma()
        done = 0
        try:
            for c in changes:
                key = c.data["key"]
                if c.op == "create":
                    res = k.call("add", DEFAULTS | {"notificationIDList": notif} | want.get(key, c.after))
                elif c.op == "update":
                    cur = k.monitors.get(int(c.data["id"])) or {}
                    res = k.call("editMonitor", cur | want.get(key, {}) | {"id": int(c.data["id"])})
                else:
                    res = k.call("deleteMonitor", int(c.data["id"]))
                if not res.get("ok"):
                    raise PluginError(f"{c.op} {c.name}: {res.get('msg') or res}")
                self.log(f"{c.op} {c.name}")
                done += 1
        finally:
            k.close()
        return {"changed": done}

    # --------------------------------------------------------------- status
    def _states(self):
        """(key, monitor, status) for the managed monitors, plus unmanaged ones matched by URL/host."""
        k = self._kuma()
        try:
            time.sleep(0.2)  # heartbeat lists follow the monitor list
            out = []
            for mid, m in k.monitors.items():
                desc = str(m.get("description") or "")
                key = desc[len(MARK):] if desc.startswith(MARK) else None
                out.append((key, m, k.status(mid)))
            return out
        finally:
            k.close()

    def run_poll(self, params):
        match = self.ctx.match_host()
        per_host: dict[str, dict] = {}
        for key, m, st in self._states():
            if key:
                host, svc = key.split("/", 1)
            else:
                host, svc = match(m.get("url") or m.get("hostname") or ""), m.get("name", "?")
            if not host:
                continue
            per_host.setdefault(host, {})[svc] = st
        report = {}
        for host, svcs in per_host.items():
            states = set(svcs.values())
            down = sorted(s for s, v in svcs.items() if v == "down")
            if "up" in states:
                state = "up"
            elif "down" in states:
                state = "down"
            else:
                state = "unknown"
            detail = f"{sum(1 for v in svcs.values() if v == 'up')}/{len(svcs)} monitors up" + (f"; down: {', '.join(down)}" if down else "")
            report[host] = {"state": state, "detail": detail, "data": {"services": svcs}}
        self.ctx.report_status(report)
        return {"hosts": len(report), "monitors": sum(len(v) for v in per_host.values())}

    # -------------------------------------------------------------- widgets
    def widget_monitors(self, host):
        states = self._states()
        items = [{"label": m.get("name", "?"), "state": {"up": "up", "down": "down", "pending": "warn", "maintenance": "warn"}.get(st, "unknown"),
                  "detail": st, "link": f"/hosts/{key.split('/')[0]}?tab=services" if key else ""} for key, m, st in states]
        items.sort(key=lambda i: ({"down": 0, "warn": 1}.get(i["state"], 2), i["label"]))
        down = sum(1 for i in items if i["state"] == "down")
        return {"kind": "status", "items": items[:25], "sub": f"{len(items)} monitors, {down} down" if items else "no monitors yet"}

    def widget_host(self, host):
        items = [{"label": m.get("name", "?"), "state": {"up": "up", "down": "down"}.get(st, "warn" if st in ("pending", "maintenance") else "unknown"),
                  "detail": m.get("url") or f"{m.get('hostname', '')}:{m.get('port', '')}"}
                 for key, m, st in self._states() if key and key.split("/", 1)[0] == host["name"]]
        return {"kind": "status", "items": items, "sub": "" if items else "no monitors for this host (Sync monitors adds them)"}
