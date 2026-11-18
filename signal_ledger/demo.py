"""命令行回放合成事件，方便无需浏览器的快速验证。"""

import asyncio
import json

from .network_guard import install_loopback_only


async def replay_summary():
    from .demo_runtime import DemoSession
    session = DemoSession()
    try:
        result = await session.replay()
        return {key: result[key] for key in ("mode", "data_source", "events_per_replay", "stats", "orders")}
    finally:
        session.close()


def main():
    install_loopback_only()
    print(json.dumps(asyncio.run(replay_summary()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
