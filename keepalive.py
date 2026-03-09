import os
import sys
import time
import urllib.request
import urllib.error


def _get_target_url() -> str:
    # Prefer private-network host:port (fromService property hostport)
    hostport = (os.getenv("TARGET_HOSTPORT") or "").strip()
    path = (os.getenv("TARGET_PATH") or "/api/health").strip() or "/api/health"
    if not path.startswith("/"):
        path = "/" + path

    if hostport:
        return f"http://{hostport}{path}"

    # Fallback to explicit external URL (set in env if you want)
    base = (os.getenv("KEEPALIVE_URL") or "").strip().rstrip("/")
    if base:
        return f"{base}{path}"

    return ""


def main() -> int:
    url = _get_target_url()
    if not url:
        print("keepalive: missing TARGET_HOSTPORT or KEEPALIVE_URL", file=sys.stderr)
        return 2

    timeout = float(os.getenv("KEEPALIVE_TIMEOUT") or "10")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "jet-store-keepalive/1.0",
                "Accept": "application/json, text/plain, */*",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.getcode()
            body = resp.read(256) or b""
            try:
                body_preview = body.decode("utf-8", errors="replace").strip()
            except Exception:
                body_preview = repr(body)
            print(f"[{ts}] keepalive ok: {code} {url} | {body_preview[:200]}")
            return 0
    except urllib.error.HTTPError as e:
        print(f"[{ts}] keepalive http error: {e.code} {url}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"[{ts}] keepalive failed: {url} | {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

