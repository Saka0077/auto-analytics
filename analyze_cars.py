import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class CarListing:
    title: str
    price: float
    year: int | None
    mileage: float | None
    owners: int | None
    engine_volume: float | None
    city: str
    url: str
    description: str
    source: str
    score: float = 0.0


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = (
        str(value)
        .replace("₸", "")
        .replace(" ", "")
        .replace(",", ".")
        .strip()
    )
    digits = "".join(ch for ch in cleaned if ch.isdigit() or ch == ".")
    return float(digits) if digits else None


def parse_int(value: Any) -> int | None:
    number = parse_float(value)
    return int(number) if number is not None else None


def normalize(value: float, min_value: float, max_value: float, invert: bool = False) -> float:
    if max_value == min_value:
        return 1.0
    result = (value - min_value) / (max_value - min_value)
    return 1.0 - result if invert else result


def load_json(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
        return data["items"]
    raise ValueError("JSON должен содержать список объектов или ключ 'items'.")


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def load_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        return load_json(path)
    if path.suffix.lower() == ".csv":
        return load_csv(path)
    raise ValueError("Поддерживаются только файлы .csv и .json")


def row_to_listing(row: dict[str, Any]) -> CarListing:
    return CarListing(
        title=str(row.get("title", row.get("name", "Без названия"))).strip(),
        price=parse_float(row.get("price")) or 0.0,
        year=parse_int(row.get("year")),
        mileage=parse_float(row.get("mileage")),
        owners=parse_int(row.get("owners")),
        engine_volume=parse_float(row.get("engine_volume")),
        city=str(row.get("city", "")).strip(),
        url=str(row.get("url", "")).strip(),
        description=str(row.get("description", "")).strip(),
        source=str(row.get("source", "unknown")).strip(),
    )


def score_listings(listings: list[CarListing]) -> list[CarListing]:
    priced = [item.price for item in listings if item.price > 0]
    years = [item.year for item in listings if item.year is not None]
    mileages = [item.mileage for item in listings if item.mileage is not None]
    owners = [item.owners for item in listings if item.owners is not None]

    price_min, price_max = min(priced), max(priced)
    year_min, year_max = min(years), max(years)
    mileage_min, mileage_max = min(mileages), max(mileages)
    owners_min, owners_max = min(owners), max(owners)

    for item in listings:
        price_score = normalize(item.price, price_min, price_max, invert=True) if item.price > 0 else 0.0
        year_score = normalize(item.year, year_min, year_max) if item.year is not None else 0.5
        mileage_score = (
            normalize(item.mileage, mileage_min, mileage_max, invert=True)
            if item.mileage is not None
            else 0.5
        )
        owners_score = (
            normalize(item.owners, owners_min, owners_max, invert=True)
            if item.owners is not None
            else 0.5
        )

        item.score = round(
            price_score * 0.45
            + year_score * 0.25
            + mileage_score * 0.20
            + owners_score * 0.10,
            4,
        )

    return sorted(listings, key=lambda item: (-item.score, item.price))


def filter_listings(
    listings: list[CarListing],
    min_year: int | None,
    max_price: float | None,
    city: str | None,
) -> list[CarListing]:
    result = listings
    if min_year is not None:
        result = [item for item in result if item.year is not None and item.year >= min_year]
    if max_price is not None:
        result = [item for item in result if item.price <= max_price]
    if city:
        result = [item for item in result if item.city.lower() == city.lower()]
    return result


def print_table(listings: list[CarListing], limit: int) -> None:
    print(f"{'Score':<8} {'Price':<12} {'Year':<6} {'Mileage':<10} {'Owners':<8} Title")
    print("-" * 90)
    for item in listings[:limit]:
        year = item.year if item.year is not None else "-"
        mileage = int(item.mileage) if item.mileage is not None else "-"
        owners = item.owners if item.owners is not None else "-"
        print(
            f"{item.score:<8} {int(item.price):<12} {year!s:<6} {mileage!s:<10} {owners!s:<8} {item.title}"
        )
        if item.city or item.url:
            print(f"  {item.city} {item.url}".rstrip())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Сортирует объявления и находит лучшие варианты по цене и качеству."
    )
    parser.add_argument("input_file", help="Путь к CSV или JSON файлу с объявлениями.")
    parser.add_argument("--min-year", type=int, help="Минимальный год выпуска.")
    parser.add_argument("--max-price", type=float, help="Максимальная цена.")
    parser.add_argument("--city", help="Фильтр по городу.")
    parser.add_argument("--limit", type=int, default=10, help="Сколько объявлений показать.")
    args = parser.parse_args()

    rows = load_rows(Path(args.input_file))
    listings = [row_to_listing(row) for row in rows]
    listings = [item for item in listings if item.price > 0]
    listings = filter_listings(listings, args.min_year, args.max_price, args.city)

    if not listings:
        print("Подходящих объявлений не найдено.")
        return

    ranked = score_listings(listings)
    print_table(ranked, args.limit)


if __name__ == "__main__":
    main()
