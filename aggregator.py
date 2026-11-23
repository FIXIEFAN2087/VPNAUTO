import asyncio
import base64
import re
from pathlib import Path
from urllib.parse import urlparse
import aiohttp

INPUT_FILE = Path("a.txt")

MAX_KEYS_TO_CHECK = 500
CONCURRENCY = 30
TCP_TIMEOUT = 2.5

URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
PROXY_URI_RE = re.compile(r"(?:vless|ss)://[^\s\"'<>]+", re.IGNORECASE)

HEADERS = {
    "User-Agent": "HiddifyNext/2.5.7 (Android) like v2rayNG",
    "Accept": "*/*",
}


def decode_if_base64(text: str) -> str:
    cleaned = "".join(text.strip().split())
    if not cleaned or "vless://" in text.lower() or "ss://" in text.lower():
        return text
    padded = cleaned + "=" * (-len(cleaned) % 4)
    try:
        decoded = base64.b64decode(padded, validate=False).decode("utf-8", errors="ignore")
        if "vless://" in decoded.lower() or "ss://" in decoded.lower():
            return decoded
    except Exception:
        pass
    return text


def parse_proxy_target(uri: str) -> tuple[str, int] | None:
    try:
        parsed = urlparse(uri.strip())
        scheme = parsed.scheme.lower()
        if scheme not in {"vless", "ss"}:
            return None

        if parsed.hostname and parsed.port:
            if 1 <= parsed.port <= 65535:
                return parsed.hostname.strip("[]"), int(parsed.port)
            return None

        rest = uri.split("://", 1)[1].split("#", 1)[0].split("?", 1)[0]
        if "@" not in rest:
            padded = rest + "=" * (-len(rest) % 4)
            decoded = base64.b64decode(padded, validate=False).decode("utf-8", errors="ignore")
            if "@" in decoded:
                host_port = decoded.rsplit("@", 1)[1]
                host, port_str = host_port.rsplit(":", 1)
                port = int(port_str)
                if host and 1 <= port <= 65535:
                    return host.strip("[]"), port
    except Exception:
        pass
    return None


def extract_proxies_from_text(text: str) -> set[str]:
    found: set[str] = set()
    expanded = f"{text}\n{decode_if_base64(text)}"
    for match in PROXY_URI_RE.findall(expanded):
        clean_uri = match.strip().rstrip(",;")
        if parse_proxy_target(clean_uri):
            found.add(clean_uri)
    return found


def split_input_lines(raw_text: str) -> tuple[list[str], str]:
    """Отделяет URL подписок от остального текста (прямых ключей / Base64)."""
    sub_urls: list[str] = []
    seen_urls: set[str] = set()
    leftover_lines: list[str] = []

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        urls_in_line = URL_RE.findall(stripped)
        if urls_in_line:
            for u in urls_in_line:
                clean_u = u.rstrip(",;")
                if clean_u not in seen_urls:
                    seen_urls.add(clean_u)
                    sub_urls.append(clean_u)
        else:
            leftover_lines.append(stripped)

    return sub_urls, "\n".join(leftover_lines)


async def fetch_subscription(
    session: aiohttp.ClientSession, url: str
) -> tuple[str, set[str]]:
    try:
        async with session.get(
            url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            if resp.status != 200:
                print(f"[SUB] {url} -> HTTP {resp.status}")
                return url, set()
            body = await resp.text(errors="ignore")
            keys = extract_proxies_from_text(body)
            print(f"[SUB] {url} -> найдено ключей: {len(keys)}")
            return url, keys
    except Exception as exc:
        print(f"[SUB] {url} -> ошибка: {exc}")
        return url, set()


async def check_tcp(sem: asyncio.Semaphore, uri: str) -> tuple[str, float] | None:
    target = parse_proxy_target(uri)
    if not target:
        return None
    host, port = target
    async with sem:
        loop = asyncio.get_running_loop()
        start = loop.time()
        try:
            conn = asyncio.open_connection(host, port)
            reader, writer = await asyncio.wait_for(conn, timeout=TCP_TIMEOUT)
            latency_ms = (loop.time() - start) * 1000
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return uri, latency_ms
        except Exception:
            return None


def label_key(uri: str, latency_ms: float) -> str:
    base_part, _, _ = uri.partition("#")
    return f"{base_part}#TCP {int(latency_ms)}ms"


async def main() -> None:
    all_keys: set[str] = set()

    if INPUT_FILE.exists():
        raw_local = INPUT_FILE.read_text(encoding="utf-8", errors="ignore")
        sub_urls, leftover_text = split_input_lines(raw_local)
        all_keys.update(extract_proxies_from_text(leftover_text))
    else:
        print(f"Файл {INPUT_FILE} не найден — создайте его.")
        return

    if sub_urls:
        connector = aiohttp.TCPConnector(limit=15, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            results = await asyncio.gather(
                *(fetch_subscription(session, u) for u in sub_urls)
            )
            for _, keys in results:
                all_keys.update(keys)

    keys_list = list(all_keys)[:MAX_KEYS_TO_CHECK]
    sem = asyncio.Semaphore(CONCURRENCY)
    checked = await asyncio.gather(*(check_tcp(sem, k) for k in keys_list))

    alive = [item for item in checked if item is not None]
    alive.sort(key=lambda x: x[1])

    formatted_keys = [label_key(uri, ms) for uri, ms in alive]

    Path("sub.txt").write_text(
        "\n".join(formatted_keys) + ("\n" if formatted_keys else ""),
        encoding="utf-8",
    )

    b64_payload = base64.b64encode(
        "\n".join(formatted_keys).encode("utf-8")
    ).decode("ascii")
    Path("sub_base64.txt").write_text(b64_payload + "\n", encoding="utf-8")

    print(f"Итого уникальных ключей из всех источников: {len(all_keys)}")
    print(f"Проверено: {len(keys_list)}, живых по TCP: {len(formatted_keys)}")


if __name__ == "__main__":
    asyncio.run(main())
              
