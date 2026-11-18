"""验证离线演示、重复回放、原交易所路由、净值口径和 HTTP 入口边界。"""

import asyncio
import os
import socket
import subprocess
import sys
import unittest
from pathlib import Path
from decimal import Decimal
import httpx

from signal_ledger.demo_runtime import DemoSession, OfflineExchange
from signal_ledger.network_guard import install_loopback_only
from signal_ledger.web import create_app


class DemoTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        asyncio.get_running_loop().set_debug(False)
        install_loopback_only()
        self.session = DemoSession()

    async def asyncTearDown(self):
        self.session.close()

    async def test_replay_preserves_single_lifecycle(self):
        first = await self.session.replay()
        second = await self.session.replay()
        self.assertEqual(first["stats"], second["stats"])
        self.assertEqual(first["orders"], second["orders"])
        self.assertEqual(second["stats"]["holding"], 2)
        self.assertEqual(second["stats"]["closed"], 4)
        self.assertEqual(len(self.session.db.all_signals()), 6)

    async def test_fallback_and_close_use_same_exchange(self):
        data = await self.session.replay()
        self.assertEqual(self.session.db.get_signal("demo-002")["exchange"], "bitget")
        zeta = [order["kind"] for order in data["orders"]["bitget"] if order["symbol"] == "ZETAUSDT"]
        self.assertEqual(zeta, ["open", "close"])
        self.assertFalse(any(order["symbol"] == "ZETAUSDT" for order in data["orders"]["gate"]))

    async def test_curve_history_and_total_share_net_values(self):
        data = await self.session.replay()
        total = sum((Decimal(row["display_net"]) for row in data["history"]), Decimal("0"))
        self.assertEqual(total, Decimal(data["stats"]["realized"]))
        self.assertEqual(total, Decimal(data["curve"]["total"]))
        self.assertEqual(total, Decimal("0.5206"))
        self.assertEqual(Decimal(data["stats"]["floating"]), Decimal("2.5"))
        self.assertEqual(Decimal(data["stats"]["fees"]), Decimal("0.5694"))
        for row in data["history"]:
            expected = (Decimal(row["gross_pnl"]) - Decimal(row["open_fee"])
                        - Decimal(row["close_fee"]) - Decimal(row["funding_fee"]))
            self.assertEqual(expected, Decimal(row["display_net"]))

    async def test_concurrent_replays_do_not_duplicate_orders(self):
        await asyncio.gather(*(self.session.replay() for _ in range(3)))
        self.assertEqual(self.session.replay_count, 3)
        orders = [order for adapter in self.session.engine.exchanges.values() for order in adapter.orders]
        self.assertEqual(sum(order["kind"] == "open" for order in orders), 6)
        self.assertEqual(sum(order["kind"] == "close" for order in orders), 4)

    async def test_demo_has_no_real_adapter_or_credential(self):
        self.assertTrue(all(isinstance(adapter, OfflineExchange)
                            for adapter in self.session.engine.exchanges.values()))
        params = self.session.engine.params.all()
        for key in ("signal_api_key", "exchange_api_key", "exchange_secret",
                    "exchange_passphrase", "gate_api_key", "gate_secret"):
            self.assertEqual(params[key], "")
        self.assertTrue(params["sim_mode"])
        with self.assertRaises(RuntimeError):
            await self.session.engine.start()


class WebTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        asyncio.get_running_loop().set_debug(False)

    async def test_lifespan_uses_and_removes_private_temporary_database(self):
        app = create_app()
        async with app.router.lifespan_context(app):
            directory = Path(app.state.session.temp.name)
            self.assertTrue(directory.is_dir())
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
                response = await client.get("/api/status")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["mode"], "offline_demo")
                self.assertIn("合成", response.json()["data_source"])
                self.assertIn("基于 Python", (await client.get("/")).text)
        self.assertFalse(directory.exists())

    async def test_filters_and_invalid_exchange(self):
        app = create_app()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
                data = (await client.get("/api/status?exchange=bitget")).json()
                self.assertEqual(data["stats"]["holding"], 1)
                self.assertEqual(data["stats"]["closed"], 1)
                self.assertTrue(all(row["exchange"] == "bitget" for row in data["history"]+data["holdings"]))
                self.assertEqual((await client.get("/api/status?exchange=unknown")).status_code, 422)

    async def test_no_live_or_credentials_routes(self):
        app = create_app()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
                for route in ("/api/start", "/api/settings", "/api/apply_margin_leverage",
                              "/api/catchup_open", "/api/test_exchange", "/api/test_gate"):
                    self.assertEqual((await client.post(route, json={})).status_code, 404)

    async def test_cross_origin_replay_rejected(self):
        app = create_app()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
                before = (await client.get("/api/status")).json()["replay_count"]
                response = await client.post("/api/demo/replay", headers={"origin": "https://untrusted.example.invalid"})
                self.assertEqual(response.status_code, 403)
                self.assertEqual((await client.get("/api/status")).json()["replay_count"], before)
                self.assertEqual((await client.post("/api/demo/replay")).status_code, 200)

    async def test_live_environment_rejected_by_cli(self):
        env = dict(os.environ, TRADE_MODE="live", PYTHONDONTWRITEBYTECODE="1", PYTHONUTF8="1")
        result = subprocess.run([sys.executable, "-m", "signal_ledger"], env=env,
                                capture_output=True, text=True, encoding="utf-8", timeout=15)
        self.assertEqual(result.returncode, 2)
        self.assertIn("TRADE_MODE=live", result.stderr)

    async def test_outbound_connect_and_dns_denied(self):
        install_loopback_only()
        with self.assertRaises(PermissionError):
            socket.getaddrinfo("api.bitget.com", 443)
        with socket.socket() as connection:
            with self.assertRaises(PermissionError):
                connection.connect(("203.0.113.1", 443))
