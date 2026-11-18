"""仅在本机回环地址启动公开演示。"""

import argparse
import os

from .network_guard import install_loopback_only


def main():
    parser = argparse.ArgumentParser(description="多交易所信号执行与账本管理系统：离线演示")
    parser.add_argument("--port", type=int, default=8877, help="本机演示端口，默认 8877")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("端口必须在 1～65535 之间")
    if os.environ.get("TRADE_MODE", "").lower().strip() == "live":
        parser.error("公开展示入口拒绝在 TRADE_MODE=live 环境中运行")
    install_loopback_only()
    import uvicorn
    from .web import create_app
    print(f"离线演示：http://127.0.0.1:{args.port}；行情、订单、盈亏全部为合成数据。")
    uvicorn.run(create_app(), host="127.0.0.1", port=args.port, access_log=False)


if __name__ == "__main__":
    main()
