# Data Schema V1

Дата: 12 марта 2026

Этот документ фиксирует минимальную рабочую схему данных для проекта.
Источник объявлений: в первую очередь `kolesa.kz`.

## 1. `listings`

Главная сущность. Одна запись = одно текущее объявление.

Обязательные поля:

```json
{
  "id": "kolesa:213912409",
  "advert_id": "213912409",
  "source": "kolesa.kz",
  "url": "https://kolesa.kz/a/show/213912409",
  "title": "Toyota Camry",
  "price": 10700000,
  "city": "Алматы",
  "year": 2016,
  "mileage": 156000,
  "brand": "Toyota",
  "model": "Camry",
  "body_type": "седан",
  "fuel_type": "Бензин",
  "transmission": "Автомат",
  "engine_volume": 2.5,
  "description": "2016 г., Б/у седан, 2.5 л, бензин...",
  "image": "https://kolesa-photos.kcdn.online/...jpg",
  "photo_gallery": [
    "https://kolesa-photos.kcdn.online/...jpg"
  ],
  "photo_count": 1,
  "publication_date": "2026-03-12T02:47:09+05:00",
  "last_update": "2026-03-12T02:47:09+05:00",
  "actuality_status": "active",
  "credit_available": false,
  "avg_price": 11250000,
  "market_difference": 550000,
  "market_difference_percent": 4.9,
  "autoparts_profile_id": "toyota-camry-xv50-2.5-at-kz",
  "first_seen_at": "2026-03-12T10:03:00+05:00",
  "last_seen_at": "2026-03-12T10:03:00+05:00",
  "last_checked_at": "2026-03-12T10:03:00+05:00"
}
```

Желательные поля:

```json
{
  "generation": "XV50",
  "trim": "Comfort",
  "drive_type": "Передний",
  "steering_side": "Левый",
  "color": "Черный",
  "repair_state": "unknown",
  "promotion_type": "top",
  "credit_monthly_payment": 289000,
  "credit_down_payment": 3200000,
  "seller_type_id": 6,
  "seller_name": "Частное лицо",
  "phone_count": 1,
  "views_count": 248,
  "history_auto_present": true,
  "options": ["кожа", "камера заднего вида"],
  "vin_note": "",
  "risk_score": 0.28,
  "buyer_score": 0.81,
  "reseller_opportunity_score": 0.57,
  "maintenance_score": 0.73,
  "liquidity_score": 0.69,
  "deal_score": 0.76,
  "bad_score": 0.18
}
```

Не делать обязательными в V1:

```json
{
  "vin": "",
  "owners_count": null,
  "full_phone": "",
  "official_history_payload": null
}
```

## 2. `listing_snapshots`

История наблюдения за объявлением. Одна запись = один момент проверки.

Нужна для:
- дней в продаже
- истории цены
- истории статуса
- оценки ликвидности

```json
{
  "id": "snapshot:kolesa:213912409:2026-03-12T10:03:00+05:00",
  "listing_id": "kolesa:213912409",
  "captured_at": "2026-03-12T10:03:00+05:00",
  "price": 10700000,
  "publication_date": "2026-03-12T02:47:09+05:00",
  "last_update": "2026-03-12T02:47:09+05:00",
  "actuality_status": "active",
  "views_count": 248,
  "promotion_type": "top",
  "photo_count": 1,
  "credit_available": false,
  "credit_monthly_payment": null,
  "city": "Алматы",
  "raw_hash": "e3ec2d2e..."
}
```

Минимальные derived-поля, которые потом считаются из snapshots:

```json
{
  "days_on_market": 6,
  "price_change_count": 2,
  "first_price": 11200000,
  "last_price": 10700000,
  "price_drop_total": 500000,
  "was_relisted": false
}
```

## 3. `model_catalog`

Нормализованный каталог моделей. Это не объявление, а эталон машины.

Нужен для:
- связки с запчастями
- точного матчинга поколения
- будущего каталога по моделям

```json
{
  "id": "toyota-camry-xv50-2015-2017-kz",
  "market": "KZ",
  "make": "Toyota",
  "model": "Camry",
  "generation": "XV50",
  "model_year_from": 2015,
  "model_year_to": 2017,
  "trim": "2.5 AT Comfort",
  "body_type": "седан",
  "fuel_type": "Бензин",
  "hybrid_type": "",
  "drive_type": "Передний",
  "transmission": "Автомат",
  "doors": 4,
  "seats": 5,
  "length_mm": 4850,
  "width_mm": 1825,
  "height_mm": 1480,
  "wheelbase_mm": 2775,
  "ground_clearance_mm": 160,
  "boot_l_seats_up": 506,
  "boot_l_seats_down": null,
  "boot_measurement_method": "",
  "passenger_volume_l": null,
  "engine_cc": 2494,
  "power_hp": 181,
  "torque_nm": 231,
  "fuel_tank_l": 70,
  "battery_kwh": null,
  "range_wltp_km": null,
  "consumption_l_100km": 8.3,
  "consumption_kwh_100km": null,
  "co2_g_km": null,
  "isofix_positions": 2,
  "latch_rating": "",
  "euro_ncap_stars": null,
  "euro_ncap_child_score": null,
  "nhtsa_stars": null,
  "source_name": "manual_seed",
  "source_url": "",
  "source_date": "2026-03-12",
  "raw_source_text": ""
}
```

Для V1 критично хранить именно это ядро:
- `market`
- `make`
- `model`
- `generation`
- `model_year_from`
- `model_year_to`
- `trim`
- `body_type`
- `fuel_type`
- `drive_type`
- `transmission`
- `engine_cc`
- `power_hp`
- `consumption_l_100km`

## 4. `autoparts_profiles`

Нормализованный профиль стоимости обслуживания модели.

Нужен для:
- `maintenance score`
- фильтра `дешёвые в обслуживании`
- сигнала для покупателя и перекупа

```json
{
  "id": "toyota-camry-xv50-2.5-at-kz",
  "market": "KZ",
  "make": "Toyota",
  "model": "Camry",
  "generation": "XV50",
  "model_year_from": 2015,
  "model_year_to": 2017,
  "trim": "2.5 AT",
  "engine_cc": 2494,
  "transmission": "Автомат",
  "front_pads_price_kzt": 12500,
  "front_pads_stock": 18,
  "front_disc_price_kzt": 26000,
  "front_disc_stock": 9,
  "service_basket_kzt": 64500,
  "avg_stock": 13.5,
  "price_score": 0.58,
  "cheapness_score": 0.54,
  "maintenance_band": "medium",
  "coverage_level": "full",
  "priority": 2,
  "source_name": "kz_autoparts_market_snapshot_2026_03_12",
  "source_url": "",
  "source_date": "2026-03-12",
  "raw_source_text": ""
}
```

Минимум для V1:
- `make`
- `model`
- `generation`
- `model_year_from`
- `model_year_to`
- `engine_cc`
- `transmission`
- `front_pads_price_kzt`
- `front_disc_price_kzt`
- `service_basket_kzt`
- `avg_stock`
- `cheapness_score`
- `maintenance_band`
- `coverage_level`

## 5. Связи между сущностями

```text
listings.autoparts_profile_id -> autoparts_profiles.id
listing_snapshots.listing_id -> listings.id
listings.brand/model/generation/year -> model_catalog
autoparts_profiles.make/model/generation/year -> model_catalog
```

Практически:
- `listing` живёт как текущее состояние объявления
- `snapshot` хранит историю
- `model_catalog` описывает машину как модель
- `autoparts_profiles` описывает стоимость содержания модели

## 6. Что реально внедрять первым

Сначала:
1. стабилизировать `listings`
2. начать записывать `listing_snapshots`
3. расширять `autoparts_profiles`

Потом:
1. усилить `model_catalog`
2. считать историю цены
3. считать дни в продаже
4. считать более честную ликвидность

## 7. Минимальный итог для продукта

Если эта схема соблюдается, продукт уже может честно отвечать:
- что лучше купить
- что лучше для перепродажи
- где риск
- где цена ниже рынка
- где обслуживание дешёвое
- сколько объявление живёт на рынке
