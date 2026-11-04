"""E2E-проверка запущенной системы (docker compose up). На хосте нужен только Python 3 и Docker.

    python3 tests/e2e.py                 # функциональные проверки, кеш, гонка, gRPC-контракт
    python3 tests/e2e.py --resilience    # плюс отказы: Flight Service, Redis master, БД бронирований

--resilience останавливает и снова запускает контейнеры; занимает около минуты.
Каждый запуск создаёт свои рейсы (INSERT в flight-db), поэтому проверки можно повторять.
"""

import json
import os
import random
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

BASE_URL = os.environ.get("BOOKING_URL", "http://localhost:8080")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB_OPEN_TIMEOUT_SECONDS = 15  # из docker-compose.yml
SEED_FLIGHT = "a0000000-0000-4000-8000-000000000001"  # SU1234 SVO -> LED 2026-12-01

passed = 0


# ---------- helpers ----------


def call(method, path, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE_URL + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status, raw, hdrs = resp.status, resp.read(), resp.headers
    except urllib.error.HTTPError as e:
        status, raw, hdrs = e.code, e.read(), e.headers
    return status, json.loads(raw or b"null"), hdrs, time.perf_counter() - started


def check(name, condition, detail=""):
    global passed
    if not condition:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)
    passed += 1
    print(f"ok   {name}")


def expect(name, response, status, error_code=None):
    got_status, body = response[0], response[1]
    ok = got_status == status and (error_code is None or (body or {}).get("error_code") == error_code)
    check(f"{name} -> {status}{' ' + error_code if error_code else ''}", ok, f"got {got_status} {json.dumps(body, ensure_ascii=False)}")
    return body


def compose(*args, check_rc=True, **kwargs):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=check_rc, capture_output=True, text=True, **kwargs)


def flight_sql(statement):
    out = compose("exec", "-T", "flight-db", "psql", "-U", "flights", "-d", "flights", "-tAc", statement).stdout
    return out.strip().splitlines()[0] if out.strip() else ""


def new_flight(seats, price="5000.00", origin="DME", destination="KZN"):
    """Рейс с уникальной парой (номер, дата), чтобы прогоны не мешали друг другу."""
    day = f"2027-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
    return flight_sql(
        "INSERT INTO flights (airline_code, flight_no, origin, destination, departure_time, arrival_time, "
        "total_seats, available_seats, price, currency) VALUES "
        f"('S7', {random.randint(1, 9999)}, '{origin}', '{destination}', '{day}T07:00:00Z', '{day}T08:30:00Z', "
        f"{seats}, {seats}, {price}, 'RUB') RETURNING id"
    )


def booking_body(flight_id, seats=1, user="e2e-user"):
    return {
        "user_id": user, "flight_id": flight_id, "passenger_name": "Тест Тестов",
        "passenger_email": "test@example.com", "seat_count": seats,
    }


def available(flight_id):
    return call("GET", f"/flights/{flight_id}")[1]["available_seats"]


def logs_since(service, since):
    return compose("logs", "--no-log-prefix", "--since", since, service).stdout


def wait_until(predicate, timeout_s, step_s=0.5):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step_s)
    return False


# ---------- сценарии ----------


def test_flights():
    expect("GET /health", call("GET", "/health"), 200)
    flights = expect("GET /flights SVO->LED 2026-12-01", call("GET", "/flights?origin=SVO&destination=LED&date=2026-12-01"), 200)
    check("search returns only SCHEDULED flights on that date",
          [f["flight_number"] for f in flights] == ["SU1234", "SU1236"] and all(f["status"] == "SCHEDULED" for f in flights),
          [f["flight_number"] for f in flights])
    all_dates = expect("GET /flights without date, lower-case IATA", call("GET", "/flights?origin=svo&destination=led"), 200)
    check("search without date covers all dates", len(all_dates) >= 3 and {f["departure_time"][:10] for f in all_dates} >= {"2026-12-01", "2026-12-02"})
    expect("GET /flights without destination", call("GET", "/flights?origin=SVO"), 400, "VALIDATION_ERROR")
    expect("GET /flights bad IATA", call("GET", "/flights?origin=SV1&destination=LED"), 400, "VALIDATION_ERROR")
    expect("GET /flights bad date", call("GET", "/flights?origin=SVO&destination=LED&date=2026-13-01"), 400, "VALIDATION_ERROR")
    expect("GET /flights origin == destination", call("GET", "/flights?origin=SVO&destination=SVO"), 400, "VALIDATION_ERROR")

    flight = expect("GET /flights/{id}", call("GET", f"/flights/{SEED_FLIGHT}"), 200)
    check("flight has price, currency, IATA codes",
          flight["price"] == "6500.00" and flight["currency"] == "RUB" and flight["origin"]["code"] == "SVO")
    expect("GET /flights/{unknown}", call("GET", f"/flights/{uuid.uuid4()}"), 404, "FLIGHT_NOT_FOUND")
    expect("GET /flights/{not-a-uuid}", call("GET", "/flights/123"), 404, "FLIGHT_NOT_FOUND")


def test_bookings():
    flight_id = new_flight(seats=10, price="4999.99")
    user = f"user-{uuid.uuid4().hex[:8]}"

    booking = expect("POST /bookings", call("POST", "/bookings", booking_body(flight_id, 3, user)), 201)
    check("booking CONFIRMED, total_price = 3 * 4999.99", booking["status"] == "CONFIRMED" and booking["total_price"] == "14999.97",
          booking)
    check("seats reserved: available 10 -> 7", available(flight_id) == 7)
    check("reservation ACTIVE in flight-db",
          flight_sql(f"SELECT status || ':' || seat_count FROM seat_reservations WHERE booking_id = '{booking['id']}'") == "ACTIVE:3")

    expect("GET /bookings/{id}", call("GET", f"/bookings/{booking['id']}"), 200)
    expect("GET /bookings/{unknown}", call("GET", f"/bookings/{uuid.uuid4()}"), 404, "BOOKING_NOT_FOUND")

    expect("POST /bookings not enough seats", call("POST", "/bookings", booking_body(flight_id, 8, user)), 409, "NOT_ENOUGH_SEATS")
    expect("POST /bookings unknown flight", call("POST", "/bookings", booking_body(str(uuid.uuid4()), 1, user)), 404, "FLIGHT_NOT_FOUND")
    expect("POST /bookings CANCELLED flight",
           call("POST", "/bookings", booking_body("a0000000-0000-4000-8000-000000000003", 1, user)), 409, "FLIGHT_NOT_BOOKABLE")
    expect("POST /bookings seat_count 0", call("POST", "/bookings", booking_body(flight_id, 0, user)), 400, "VALIDATION_ERROR")
    expect("POST /bookings bad email",
           call("POST", "/bookings", {**booking_body(flight_id, 1, user), "passenger_email": "nope"}), 400, "VALIDATION_ERROR")
    expect("POST /bookings missing passenger_name",
           call("POST", "/bookings", {k: v for k, v in booking_body(flight_id, 1, user).items() if k != "passenger_name"}),
           400, "VALIDATION_ERROR")
    check("failed attempts changed nothing: available still 7", available(flight_id) == 7)

    listed = expect("GET /bookings?user_id", call("GET", f"/bookings?user_id={user}"), 200)
    check("failed attempts created no bookings: user has exactly 1", [b["id"] for b in listed] == [booking["id"]], listed)
    expect("GET /bookings without user_id", call("GET", "/bookings"), 400, "VALIDATION_ERROR")

    cancelled = expect("POST /bookings/{id}/cancel", call("POST", f"/bookings/{booking['id']}/cancel"), 200)
    check("booking CANCELLED, seats returned: available 10", cancelled["status"] == "CANCELLED" and available(flight_id) == 10)
    check("reservation RELEASED in flight-db",
          flight_sql(f"SELECT status FROM seat_reservations WHERE booking_id = '{booking['id']}'") == "RELEASED")
    expect("POST /bookings/{id}/cancel again", call("POST", f"/bookings/{booking['id']}/cancel"), 409, "BOOKING_NOT_CONFIRMED")
    expect("POST /bookings/{unknown}/cancel", call("POST", f"/bookings/{uuid.uuid4()}/cancel"), 404, "BOOKING_NOT_FOUND")

    status, _, headers, _ = call("GET", "/health", headers={"X-Request-Id": "e2e-trace-1"})
    check("X-Request-Id echoed in response", headers.get("X-Request-Id") == "e2e-trace-1")


def test_cache():
    flight_id = new_flight(seats=5)
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))
    call("GET", f"/flights/{flight_id}")
    call("GET", f"/flights/{flight_id}")
    key = f"flight:{flight_id}"
    events = [json.loads(line).get("event") for line in logs_since("flight-service", since).splitlines()
              if line.startswith("{") and key in line and "cache_" in line]
    check("cache-aside: first GET miss, second GET hit", events[:2] == ["cache_miss", "cache_hit"], events)

    master = compose("exec", "-T", "redis-sentinel-1", "redis-cli", "-p", "26379",
                     "sentinel", "get-master-addr-by-name", "mymaster").stdout.split()[0]
    ttl = int(compose("exec", "-T", master, "redis-cli", "TTL", key).stdout.strip())
    check(f"{key} has TTL (0 < {ttl} <= 300 s)", 0 < ttl <= 300)

    call("POST", "/bookings", booking_body(flight_id, 2))
    check("after ReserveSeats cached flight is invalidated: available 3", available(flight_id) == 3)
    search = call("GET", "/flights?origin=DME&destination=KZN")[1]
    check("search result reflects reservation", any(f["id"] == flight_id and f["available_seats"] == 3 for f in search))


def test_race_for_last_seats():
    seats, clients = 3, 12
    flight_id = new_flight(seats=seats)
    with ThreadPoolExecutor(max_workers=clients) as pool:
        results = list(pool.map(lambda i: call("POST", "/bookings", booking_body(flight_id, 1, f"racer-{i}")), range(clients)))
    statuses = sorted(r[0] for r in results)
    check(f"{clients} parallel bookings for {seats} seats: exactly {seats} x 201, rest 409",
          statuses == [201] * seats + [409] * (clients - seats), statuses)
    check("available_seats = 0, never negative", available(flight_id) == 0)
    check("exactly 3 ACTIVE reservations",
          flight_sql(f"SELECT count(*) FROM seat_reservations WHERE flight_id = '{flight_id}' AND status = 'ACTIVE'") == "3")


def test_grpc_contract():
    flight_id = new_flight(seats=50)
    env_key = compose("exec", "-T", "booking-service", "printenv", "FLIGHT_SERVICE_API_KEY").stdout.strip()
    with open(os.path.join(ROOT, "tests", "grpc_checks.py"), encoding="utf-8") as script:
        result = compose("exec", "-T", "-e", f"FLIGHT_ID={flight_id}", "-e", f"FLIGHT_SERVICE_API_KEY={env_key}",
                         "booking-service", "python", "-", stdin=script, check_rc=False)
    lines = [line for line in result.stdout.splitlines() if not line.startswith("{")]
    print("\n".join(lines))
    check("gRPC contract checks (tests/grpc_checks.py)", result.returncode == 0, result.stderr[-2000:])


def test_resilience():
    flight_id = new_flight(seats=20)
    call("GET", f"/flights/{flight_id}")

    # --- Retry + Circuit Breaker: Flight Service остановлен ---
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))
    compose("stop", "flight-service")
    statuses = []
    for _ in range(8):
        status, body, headers, elapsed = call("GET", f"/flights/{flight_id}")
        statuses.append((status, body.get("error_code"), (body.get("details") or {}).get("circuit_breaker"), elapsed))
        if statuses[-1][2] == "OPEN":
            break
    check("Flight Service down: 503/504 while CLOSED", all(s[0] in (503, 504) for s in statuses), statuses)
    status, body, headers, elapsed = call("GET", f"/flights/{flight_id}")
    check("circuit OPEN: immediate 503 with Retry-After, no timeout",
          status == 503 and body["details"]["circuit_breaker"] == "OPEN" and headers.get("Retry-After") and elapsed < 0.5,
          (status, body, elapsed))
    logs = logs_since("booking-service", since)
    check("logs: grpc_retry with backoff 100 ms then 200 ms",
          '"backoff_ms": 100' in logs and '"backoff_ms": 200' in logs and "grpc_retry_exhausted" in logs)
    check("logs: CLOSED -> OPEN", re.search(r'"from_state": "CLOSED", "to_state": "OPEN"', logs) is not None)

    compose("start", "flight-service")
    wait_until(lambda: "healthy" in compose("ps", "--format", "{{.Status}}", "flight-service").stdout, 30)
    time.sleep(CB_OPEN_TIMEOUT_SECONDS)
    expect("after open timeout: probe request succeeds", call("GET", f"/flights/{flight_id}"), 200)
    logs = logs_since("booking-service", since)
    check("logs: OPEN -> HALF_OPEN -> CLOSED",
          '"from_state": "OPEN", "to_state": "HALF_OPEN"' in logs and '"from_state": "HALF_OPEN", "to_state": "CLOSED"' in logs)

    # --- Компенсация: БД бронирований недоступна после успешного ReserveSeats ---
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))
    before = available(flight_id)
    compose("stop", "booking-db")
    status, body, _, _ = call("POST", "/bookings", booking_body(flight_id, 2))
    compose("start", "booking-db")
    check("booking DB down: POST /bookings fails with 500", status == 500, (status, body))
    check("reservation compensated: available seats unchanged", available(flight_id) == before)
    check("logs: reservation_compensated", "reservation_compensated" in logs_since("booking-service", since))
    wait_until(lambda: call("GET", "/bookings?user_id=e2e-user")[0] == 200, 30)

    # --- Redis Sentinel failover ---
    master = compose("exec", "-T", "redis-sentinel-1", "redis-cli", "-p", "26379",
                     "sentinel", "get-master-addr-by-name", "mymaster").stdout.split()[0]
    compose("stop", master)
    ok_during = all(call("GET", f"/flights/{flight_id}")[0] == 200 for _ in range(5))
    check(f"Redis master {master} down: Flight Service still answers from PostgreSQL", ok_during)

    def promoted():
        new = compose("exec", "-T", "redis-sentinel-1", "redis-cli", "-p", "26379",
                      "sentinel", "get-master-addr-by-name", "mymaster").stdout.split()
        return bool(new) and new[0] != master

    check("Sentinel promoted the replica", wait_until(promoted, 30))
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))
    call("GET", f"/flights/{flight_id}")
    call("GET", f"/flights/{flight_id}")
    check("cache works on the new master (cache_hit)",
          wait_until(lambda: "cache_hit" in logs_since("flight-service", since), 10))
    compose("start", master)


def main():
    if not wait_until(lambda: call("GET", "/health")[0] == 200, 60, 1):
        print("booking service is not up: docker compose up --build")
        sys.exit(1)
    test_flights()
    test_bookings()
    test_cache()
    test_race_for_last_seats()
    test_grpc_contract()
    if "--resilience" in sys.argv:
        test_resilience()
    print(f"\nall {passed} checks passed")


if __name__ == "__main__":
    main()
