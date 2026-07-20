import asyncio
import os
from time import perf_counter
from typing import Any, Optional

import httpx
import pandas as pd
from pydantic import BaseModel, Field, ValidationError

TIMEOUT = 10.0
OUTPUT_FOLDER = "output"
URLS = [
    "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=Asia/Seoul",
    "https://countries.dev/alpha/KOR",
    "http://ip-api.com/json/8.8.8.8",
]


class WeatherHourly(BaseModel):
    temperature_2m: list[float]
    precipitation_probability: list[int]


class WeatherResponse(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    timezone: str
    hourly: WeatherHourly


class CountryResponse(BaseModel):
    name: str
    cca2: str = Field(..., min_length=2, max_length=2)
    capital: Optional[list[str]]
    region: Optional[str]


class IpResponse(BaseModel):
    query: str
    country: str
    city: str
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    status: str = Field(..., regex="^success$")


def format_result(url: str, result: Any) -> str:
    """요청 결과를 보기 좋게 문자열로 변환"""
    if isinstance(result, Exception):
        return f"{url} -> error: {result}"
    return f"{url} -> success: {result!r}"


def validate_data(url: str, data: Any) -> Any:
    """수집한 JSON 데이터를 모델로 검증하고 필요한 필드만 추출"""
    if isinstance(data, Exception):
        return data

    if "open-meteo" in url:
        return WeatherResponse.model_validate(data).model_dump()
    if "countries.dev" in url:
        return CountryResponse.model_validate(data).model_dump()
    return IpResponse.model_validate(data).model_dump()


def partition_raw_data(raw_rows: list[tuple[int, str, Any]]) -> tuple[list[dict], list[dict]]:
    """raw_data를 valid와 errors로 분리하는 파이프라인."""
    valid: list[dict] = []
    errors: list[dict] = []

    for row, url, data in raw_rows:
        try:
            validated = validate_data(url, data)
            valid.append(validated)
        except ValidationError as exc:
            errors.append({"row": row, "error": str(exc)})

    return valid, errors


def save_valid_data_csv(valid_data: list[dict], filename: str) -> tuple[bool, float]:
    """valid 데이터를 CSV 파일로 저장하고 수행 시간을 반환합니다."""
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    csv_path = os.path.join(OUTPUT_FOLDER, filename)

    start = perf_counter()
    try:
        df = pd.DataFrame(valid_data)
        df.to_csv(csv_path, index=False)
        elapsed = perf_counter() - start
        print(f"CSV 저장 완료: {csv_path} ({elapsed:.4f}초)")
        return True, elapsed
    except Exception as exc:
        elapsed = perf_counter() - start
        print(f"CSV 저장 중 오류 발생: {exc} ({elapsed:.4f}초)")
        return False, elapsed


def save_valid_data_parquet(valid_data: list[dict], filename: str) -> tuple[bool, float]:
    """valid 데이터를 Parquet 파일로 저장하고 수행 시간을 반환합니다."""
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    parquet_path = os.path.join(OUTPUT_FOLDER, filename)

    start = perf_counter()
    try:
        df = pd.DataFrame(valid_data)
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
        elapsed = perf_counter() - start
        print(f"Parquet 저장 완료: {parquet_path} ({elapsed:.4f}초)")
        return True, elapsed
    except Exception as exc:
        elapsed = perf_counter() - start
        print(f"Parquet 저장 중 오류 발생: {exc} ({elapsed:.4f}초)")
        return False, elapsed


def read_csv_with_timing(filename: str) -> tuple[Optional[pd.DataFrame], float]:
    """CSV 파일을 읽고 수행 시간을 측정합니다."""
    csv_path = os.path.join(OUTPUT_FOLDER, filename)

    start = perf_counter()
    try:
        df = pd.read_csv(csv_path)
        elapsed = perf_counter() - start
        print(f"CSV 읽기 완료: {csv_path} ({elapsed:.4f}초)")
        return df, elapsed
    except Exception as exc:
        elapsed = perf_counter() - start
        print(f"CSV 읽기 중 오류 발생: {exc} ({elapsed:.4f}초)")
        return None, elapsed


def read_parquet_with_timing(filename: str) -> tuple[Optional[pd.DataFrame], float]:
    """Parquet 파일을 읽고 수행 시간을 측정합니다."""
    parquet_path = os.path.join(OUTPUT_FOLDER, filename)

    start = perf_counter()
    try:
        df = pd.read_parquet(parquet_path, engine="pyarrow")
        elapsed = perf_counter() - start
        print(f"Parquet 읽기 완료: {parquet_path} ({elapsed:.4f}초)")
        return df, elapsed
    except Exception as exc:
        elapsed = perf_counter() - start
        print(f"Parquet 읽기 중 오류 발생: {exc} ({elapsed:.4f}초)")
        return None, elapsed


async def fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    """AsyncClient를 사용해 URL에서 JSON 데이터 가져오기

    이 함수는 타임아웃을 적용하고 HTTP 상태를 확인한 뒤
    파싱된 JSON 데이터 반환함
    """
    response = await client.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


async def fetch_all(urls: list[str]) -> list[Any]:
    """asyncio.gather()로 URL 동시 요청"""
    async with httpx.AsyncClient() as client:
        tasks = [fetch_json(client, url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)


async def main() -> int:
    """동시 HTTP 요청 실행, 수집 데이터 검증 및 분리"""
    try:
        results = await fetch_all(URLS)
    except Exception as exc:
        print(f"요청 처리 중 예기치 않은 오류가 발생했습니다.: {exc}")
        return 1

    raw_rows: list[tuple[int, str, Any]] = []
    network_errors: list[dict] = []

    for row, (url, result) in enumerate(zip(URLS, results)):
        if isinstance(result, Exception):
            network_errors.append({"row": row, "error": str(result)})
            continue
        raw_rows.append((row, url, result))

    valid, validation_errors = partition_raw_data(raw_rows)

    print("valid:", valid)
    print("errors:", validation_errors + network_errors)

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
