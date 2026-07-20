import asyncio
from typing import Any

import httpx

TIMEOUT = 10.0
URLS = [
    "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=Asia/Seoul",
    "https://countries.dev/alpha/KOR",
    "http://ip-api.com/json/8.8.8.8",
]


def format_result(url: str, result: Any) -> str:
    """Format the fetch result for display."""
    if isinstance(result, Exception):
        return f"{url} -> error: {result}"
    return f"{url} -> success: {result!r}"


async def fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    """Fetch JSON data from the URL using an AsyncClient.

    The function applies a timeout, checks HTTP status, and returns
    parsed JSON data.
    """
    response = await client.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


async def fetch_all(urls: list[str]) -> list[Any]:
    """Fetch all URLs concurrently with asyncio.gather()."""
    async with httpx.AsyncClient() as client:
        tasks = [fetch_json(client, url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)


async def main() -> int:
    """Run concurrent HTTP requests and print the responses."""
    try:
        results = await fetch_all(URLS)
    except Exception as exc:
        print(f"Unexpected error during requests: {exc}")
        return 1

    for url, result in zip(URLS, results):
        print(format_result(url, result))

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
