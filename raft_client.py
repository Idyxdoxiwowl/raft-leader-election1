#!/usr/bin/env python3
"""
Lab 3 Raft Client Utility
Query node status and trigger actions for testing.

Usage:
  python3 raft_client.py --node http://IP:PORT status
  python3 raft_client.py --nodes http://A:8000,http://B:8001,http://C:8002 status-all
"""

from urllib import request
import argparse
import json
import sys


def http_get(url: str, timeout: float = 2.0) -> dict:
    """GET JSON from URL."""
    with request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def http_post(url: str, payload: dict, timeout: float = 2.0) -> dict:
    """POST JSON to URL."""
    data = json.dumps(payload).encode()
    req = request.Request(url, data=data, 
                          headers={"Content-Type": "application/json"}, 
                          method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def cmd_status(node: str) -> None:
    """Get status of single node."""
    url = node.rstrip("/") + "/status"
    try:
        data = http_get(url)
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def cmd_status_all(nodes: list) -> None:
    """Get status of all nodes."""
    print(f"{'Node':<6} {'Term':<6} {'Role':<12} {'Leader':<8} {'Voted For':<10}")
    print("-" * 50)
    
    for node in nodes:
        url = node.rstrip("/") + "/status"
        try:
            data = http_get(url, timeout=1.0)
            print(f"{data.get('node', '?'):<6} "
                  f"{data.get('term', '?'):<6} "
                  f"{data.get('role', '?'):<12} "
                  f"{data.get('leader') or '-':<8} "
                  f"{data.get('voted_for') or '-':<10}")
        except Exception as e:
            # Extract node ID from URL for display
            node_id = node.split(":")[-1]
            print(f"?:{node_id:<4} {'?':<6} {'UNREACHABLE':<12} {'-':<8} {'-':<10}")


def cmd_watch(nodes: list, interval: float = 1.0) -> None:
    """Continuously watch cluster status."""
    import time
    
    try:
        while True:
            print("\033[2J\033[H")  # Clear screen
            print(f"Raft Cluster Status (every {interval}s) - Ctrl+C to stop\n")
            cmd_status_all(nodes)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped.")


def main():
    parser = argparse.ArgumentParser(description="Raft Client Utility")
    parser.add_argument("--node", help="Single node URL")
    parser.add_argument("--nodes", help="Comma-separated node URLs")
    parser.add_argument("cmd", choices=["status", "status-all", "watch"])
    parser.add_argument("--interval", type=float, default=1.0, 
                        help="Watch interval in seconds")
    args = parser.parse_args()
    
    if args.cmd == "status":
        if not args.node:
            print("--node required for status command")
            sys.exit(2)
        cmd_status(args.node)
    
    elif args.cmd == "status-all":
        if not args.nodes:
            print("--nodes required for status-all command")
            sys.exit(2)
        nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]
        cmd_status_all(nodes)
    
    elif args.cmd == "watch":
        if not args.nodes:
            print("--nodes required for watch command")
            sys.exit(2)
        nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]
        cmd_watch(nodes, args.interval)


if __name__ == "__main__":
    main()
