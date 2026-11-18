"""限制演示进程的出站网络，仅保留本机浏览器访问所需的回环连接。"""

import ipaddress
import sys

_installed = False


def _local(host):
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(str(host).split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def install_loopback_only():
    """给当前专用演示或测试进程添加不可撤销的 Python 审计限制。"""
    global _installed
    if _installed:
        return

    def guard(event, args):
        if event == "socket.connect":
            address = args[1]
            if isinstance(address, tuple) and not _local(address[0]):
                raise PermissionError("公开演示版禁止连接外部网络")
        elif event == "socket.getaddrinfo" and not _local(args[0]):
            raise PermissionError("公开演示版禁止解析外部网络地址")

    sys.addaudithook(guard)
    _installed = True
