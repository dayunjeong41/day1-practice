import pytest
from pydantic import ValidationError

from main import partition_raw_data, validate_data


def test_validate_open_meteo_data() -> None:
    raw_data = {
        "latitude": 37.5665,
        "longitude": 126.9780,
        "timezone": "Asia/Seoul",
        "hourly": {
            "temperature_2m": [15.0, 16.2],
            "precipitation_probability": [0, 20],
        },
    }

    validated = validate_data("https://api.open-meteo.com/v1/forecast", raw_data)

    assert isinstance(validated, dict)
    assert validated["latitude"] == 37.5665
    assert validated["hourly"]["temperature_2m"] == [15.0, 16.2]
    assert validated["hourly"]["precipitation_probability"] == [0, 20]


def test_validate_data_raises_validation_error() -> None:
    invalid_data = {
        "latitude": "invalid",
        "longitude": 126.9780,
        "timezone": "Asia/Seoul",
        "hourly": {
            "temperature_2m": [15.0],
            "precipitation_probability": [0],
        },
    }

    with pytest.raises(ValidationError):
        validate_data("https://api.open-meteo.com/v1/forecast", invalid_data)


def test_validate_countries_dev_data() -> None:
    raw_data = {
        "name": "Korea (Republic of)",
        "alpha2Code": "KR",
        "capital": "Seoul",
        "region": "Asia",
    }

    validated = validate_data("https://countries.dev/alpha/KOR", raw_data)

    assert validated["name"] == "Korea (Republic of)"
    assert validated["alpha2_code"] == "KR"
    assert validated["capital"] == "Seoul"


def test_partition_raw_data_separates_valid_and_errors() -> None:
    raw_rows = [
        (
            0,
            "https://api.open-meteo.com/v1/forecast",
            {
                "latitude": 37.5665,
                "longitude": 126.9780,
                "timezone": "Asia/Seoul",
                "hourly": {
                    "temperature_2m": [10.0],
                    "precipitation_probability": [0],
                },
            },
        ),
        (
            1,
            "https://api.open-meteo.com/v1/forecast",
            {
                "latitude": "bad",
                "longitude": 126.9780,
                "timezone": "Asia/Seoul",
                "hourly": {
                    "temperature_2m": [10.0],
                    "precipitation_probability": [0],
                },
            },
        ),
    ]

    valid, errors = partition_raw_data(raw_rows)

    assert len(valid) == 1
    assert len(errors) == 1
    assert errors[0]["row"] == 1
    assert "error" in errors[0]
