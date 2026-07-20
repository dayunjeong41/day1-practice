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
    """요청 결과를 보기 좋게 문자열로 변환합니다."""
    if isinstance(result, Exception):
        return f"{url} -> error: {result}"
    return f"{url} -> success: {result!r}"


async def fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    """AsyncClient를 사용해 URL에서 JSON 데이터를 가져옵니다.

    이 함수는 타임아웃을 적용하고 HTTP 상태를 확인한 뒤
    파싱된 JSON 데이터를 반환합니다.
    """
    response = await client.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


async def fetch_all(urls: list[str]) -> list[Any]:
    """asyncio.gather()로 URL을 동시에 요청합니다."""
    async with httpx.AsyncClient() as client:
        tasks = [fetch_json(client, url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)


async def main() -> int:
    """동시 HTTP 요청을 실행하고 결과를 출력합니다."""
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
