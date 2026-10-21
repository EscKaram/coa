"""E2E-проверка запущенного сервиса (docker compose up). Только стандартная библиотека.

Запуск: python3 tests/e2e.py [base_url]

Ограничение частоты заказов (ORDER_RATE_LIMIT_MINUTES) проверяется сценарием «два заказа подряд»,
поэтому каждый сценарий создания заказа использует нового пользователя.
"""

import base64
import hashlib
import hmac
import json
import subprocess
import time
import sys
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
ADMIN = ("admin@marketplace.local", "admin12345")
JWT_SECRET = "change-me-in-production-please-32b"  # из docker-compose.yml

passed = 0


def call(method, path, body=None, token=None, raw=None):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(BASE_URL + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read() or b"null"), resp.headers
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null"), e.headers


def sql(statement):
    subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "marketplace", "-d", "marketplace", "-c", statement],
        check=True,
        capture_output=True,
    )


def check(name, status, body, expected_status, expected_code=None):
    global passed
    ok = status == expected_status and (expected_code is None or (body or {}).get("error_code") == expected_code)
    if not ok:
        print(f"FAIL {name}: got {status} {json.dumps(body, ensure_ascii=False)}")
        sys.exit(1)
    passed += 1
    print(f"ok   {name} -> {status}{' ' + expected_code if expected_code else ''}")
    return body


def expired_access_token():
    def b64(data):
        return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()

    now = int(time.time())
    claims = {"sub": str(uuid.uuid4()), "role": "USER", "type": "access", "iat": now - 3600, "exp": now - 60}
    signing_input = f"{b64({'alg': 'HS256', 'typ': 'JWT'})}.{b64(claims)}"
    signature = hmac.new(JWT_SECRET.encode(), signing_input.encode(), hashlib.sha256).digest()
    return signing_input + "." + base64.urlsafe_b64encode(signature).rstrip(b"=").decode()


def new_user(role="USER"):
    email = f"{role.lower()}-{uuid.uuid4().hex[:8]}@test.local"
    check(f"register {role}", *call("POST", "/auth/register", {"email": email, "password": "password123", "role": role})[:2], 201)
    _, tokens, _ = call("POST", "/auth/login", {"email": email, "password": "password123"})
    return tokens["access_token"], tokens["refresh_token"]


def product(token, **overrides):
    body = {"name": "Phone", "description": "Test", "price": 100.00, "stock": 10, "category": "electronics", "status": "ACTIVE"}
    body.update(overrides)
    return check("create product", *call("POST", "/products", body, token)[:2], 201)


def promo(token, **overrides):
    now = datetime.now(timezone.utc)
    body = {
        "code": "P" + uuid.uuid4().hex[:8].upper(),
        "discount_type": "PERCENTAGE",
        "discount_value": 10,
        "min_order_amount": 0,
        "max_uses": 5,
        "valid_from": (now - timedelta(days=1)).isoformat(),
        "valid_until": (now + timedelta(days=1)).isoformat(),
    }
    body.update(overrides)
    return check("create promo", *call("POST", "/promo-codes", body, token)[:2], 201)


def main():
    status, body, headers = call("GET", "/health")
    check("health", status, body, 200)
    assert headers.get("X-Request-Id"), "X-Request-Id header is missing"

    # ---------- auth ----------
    check("no token", *call("GET", "/products")[:2], 401, "TOKEN_INVALID")
    check("garbage token", *call("GET", "/products", token="abc")[:2], 401, "TOKEN_INVALID")
    check("expired token", *call("GET", "/products", token=expired_access_token())[:2], 401, "TOKEN_EXPIRED")
    check("bad login", *call("POST", "/auth/login", {"email": ADMIN[0], "password": "wrong"})[:2], 401, "INVALID_CREDENTIALS")
    check("bad refresh", *call("POST", "/auth/refresh", {"refresh_token": "x.y.z"})[:2], 401, "REFRESH_TOKEN_INVALID")
    _, admin_tokens, _ = call("POST", "/auth/login", {"email": ADMIN[0], "password": ADMIN[1]})
    admin = admin_tokens["access_token"]
    check("refresh token used as access", *call("GET", "/products", token=admin_tokens["refresh_token"])[:2], 401, "TOKEN_INVALID")
    check("refresh", *call("POST", "/auth/refresh", {"refresh_token": admin_tokens["refresh_token"]})[:2], 200)

    seller, _ = new_user("SELLER")
    other_seller, _ = new_user("SELLER")
    user, _ = new_user()
    check("register as ADMIN is rejected", *call("POST", "/auth/register", {"email": "x@y.z", "password": "password123", "role": "ADMIN"})[:2], 400, "VALIDATION_ERROR")

    # ---------- products CRUD + roles ----------
    p = product(seller, price=19.99, stock=5)
    check("USER cannot create product", *call("POST", "/products", {"name": "x", "price": 1, "stock": 1, "category": "c", "status": "ACTIVE"}, user)[:2], 403, "ACCESS_DENIED")
    check("get product", *call("GET", f"/products/{p['id']}", token=user)[:2], 200)
    check("get missing product", *call("GET", f"/products/{uuid.uuid4()}", token=user)[:2], 404, "PRODUCT_NOT_FOUND")
    check("invalid uuid", *call("GET", "/products/not-a-uuid", token=user)[:2], 400, "VALIDATION_ERROR")
    upd = {"name": "Phone 2", "description": None, "price": 25.5, "stock": 7, "category": "electronics", "status": "ACTIVE"}
    check("other seller cannot update", *call("PUT", f"/products/{p['id']}", upd, other_seller)[:2], 403, "ACCESS_DENIED")
    updated = check("owner updates", *call("PUT", f"/products/{p['id']}", upd, seller)[:2], 200)
    assert updated["price"] == 25.5 and updated["updated_at"] > updated["created_at"]
    check("admin updates any", *call("PUT", f"/products/{p['id']}", upd, admin)[:2], 200)
    page = check("list with filter", *call("GET", "/products?page=0&size=2&status=ACTIVE&category=electronics", token=user)[:2], 200)
    assert page["size"] == 2 and page["page"] == 0 and "totalElements" in page
    check("list bad size", *call("GET", "/products?size=0", token=user)[:2], 400, "VALIDATION_ERROR")
    check("list bad status", *call("GET", "/products?status=FOO", token=user)[:2], 400, "VALIDATION_ERROR")

    _, body, _ = call("POST", "/products", {"name": "", "price": 0, "stock": -1, "category": "", "status": "NEW"}, seller)
    check("product validation", 400, body, 400, "VALIDATION_ERROR")
    fields = {f["field"] for f in body["details"]["fields"]}
    assert {"name", "price", "stock", "category", "status"} <= fields, fields
    check("stock above int32", *call("POST", "/products", {"name": "x", "price": 1, "stock": 3_000_000_000, "category": "c", "status": "ACTIVE"}, seller)[:2], 400, "VALIDATION_ERROR")
    check("page above int32", *call("GET", "/products?page=10000000000000000000", token=user)[:2], 400, "VALIDATION_ERROR")
    check("malformed json", *call("POST", "/products", raw=b"{oops", token=seller)[:2], 400, "VALIDATION_ERROR")
    check("missing body", *call("POST", "/products", raw=b"", token=seller)[:2], 400, "VALIDATION_ERROR")

    archived = product(seller)
    deleted = check("soft delete", *call("DELETE", f"/products/{archived['id']}", token=seller)[:2], 200)
    assert deleted["status"] == "ARCHIVED"
    check("archived still readable", *call("GET", f"/products/{archived['id']}", token=user)[:2], 200)

    # ---------- orders: create ----------
    a = product(seller, price=100, stock=10)
    b = product(seller, price=50, stock=3)
    inactive = product(seller, status="INACTIVE")
    check("SELLER cannot order", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}]}, seller)[:2], 403, "ACCESS_DENIED")
    check("order empty items", *call("POST", "/orders", {"items": []}, user)[:2], 400, "VALIDATION_ERROR")
    check("order quantity 1000", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1000}]}, user)[:2], 400, "VALIDATION_ERROR")
    check("order bad promo format", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}], "promo_code": "bad"}, user)[:2], 400, "VALIDATION_ERROR")
    check("order 51 items", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}] * 51}, user)[:2], 400, "VALIDATION_ERROR")

    u1, _ = new_user()
    check("order missing product", *call("POST", "/orders", {"items": [{"product_id": str(uuid.uuid4()), "quantity": 1}]}, u1)[:2], 404, "PRODUCT_NOT_FOUND")
    check("order inactive product", *call("POST", "/orders", {"items": [{"product_id": inactive["id"], "quantity": 1}]}, u1)[:2], 409, "PRODUCT_INACTIVE")
    check("order archived product", *call("POST", "/orders", {"items": [{"product_id": archived["id"], "quantity": 1}]}, u1)[:2], 409, "PRODUCT_INACTIVE")
    _, body, _ = call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 2}, {"product_id": b["id"], "quantity": 5}]}, u1)
    check("insufficient stock", 409, body, 409, "INSUFFICIENT_STOCK")
    assert body["details"]["items"] == [{"product_id": b["id"], "requested": 5, "available": 3}], body
    assert call("GET", f"/products/{a['id']}", token=u1)[1]["stock"] == 10, "stock must not change on failure"

    pc = promo(seller, discount_value=10)
    order = check("create order with promo", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 2}, {"product_id": b["id"], "quantity": 1}], "promo_code": pc["code"]}, u1)[:2], 201)
    assert order["status"] == "CREATED" and order["discount_amount"] == 25 and order["total_amount"] == 225, order
    assert call("GET", f"/products/{a['id']}", token=u1)[1]["stock"] == 8
    check("rate limit", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}]}, u1)[:2], 429, "ORDER_LIMIT_EXCEEDED")

    # снапшот цены: изменение цены товара не влияет на заказ
    call("PUT", f"/products/{a['id']}", {"name": "A", "price": 999, "stock": 8, "category": "electronics", "status": "ACTIVE"}, seller)
    got = check("get order", *call("GET", f"/orders/{order['id']}", token=u1)[:2], 200)
    assert got["total_amount"] == 225 and got["items"][0]["price_at_order"] == 100
    check("other user cannot read order", *call("GET", f"/orders/{order['id']}", token=user)[:2], 403, "ORDER_OWNERSHIP_VIOLATION")
    check("admin reads any order", *call("GET", f"/orders/{order['id']}", token=admin)[:2], 200)
    check("missing order", *call("GET", f"/orders/{uuid.uuid4()}", token=u1)[:2], 404, "ORDER_NOT_FOUND")

    # ---------- orders: update ----------
    check("other user cannot update", *call("PUT", f"/orders/{order['id']}", {"items": [{"product_id": b["id"], "quantity": 1}]}, user)[:2], 403, "ORDER_OWNERSHIP_VIOLATION")
    upd_order = check("update order", *call("PUT", f"/orders/{order['id']}", {"items": [{"product_id": b["id"], "quantity": 2}]}, u1)[:2], 200)
    assert upd_order["total_amount"] == 90 and upd_order["discount_amount"] == 10, upd_order
    assert call("GET", f"/products/{a['id']}", token=u1)[1]["stock"] == 10, "old items must be returned"
    assert call("GET", f"/products/{b['id']}", token=u1)[1]["stock"] == 1
    check("update rate limit", *call("PUT", f"/orders/{order['id']}", {"items": [{"product_id": b["id"], "quantity": 1}]}, u1)[:2], 429, "ORDER_LIMIT_EXCEEDED")

    # ---------- state machine + cancel ----------
    check("invalid transition CREATED->PAID", *call("POST", f"/orders/{order['id']}/status", {"status": "PAID"}, u1)[:2], 409, "INVALID_STATE_TRANSITION")
    check("CREATED->PAYMENT_PENDING", *call("POST", f"/orders/{order['id']}/status", {"status": "PAYMENT_PENDING"}, u1)[:2], 200)
    check("update in PAYMENT_PENDING", *call("PUT", f"/orders/{order['id']}", {"items": [{"product_id": b["id"], "quantity": 1}]}, admin)[:2], 409, "INVALID_STATE_TRANSITION")
    cancelled = check("cancel", *call("POST", f"/orders/{order['id']}/cancel", token=u1)[:2], 200)
    assert cancelled["status"] == "CANCELED"
    assert call("GET", f"/products/{b['id']}", token=u1)[1]["stock"] == 3, "stock must be returned on cancel"
    check("cancel twice", *call("POST", f"/orders/{order['id']}/cancel", token=u1)[:2], 409, "INVALID_STATE_TRANSITION")

    # ---------- promo codes ----------
    check("USER cannot create promo", *call("POST", "/promo-codes", {"code": "ABCD", "discount_type": "PERCENTAGE", "discount_value": 1, "max_uses": 1, "valid_from": "2026-01-01T00:00:00Z", "valid_until": "2027-01-01T00:00:00Z"}, user)[:2], 403, "ACCESS_DENIED")
    u2, _ = new_user()
    check("promo not found", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}], "promo_code": "NOPE_CODE"}, u2)[:2], 422, "PROMO_CODE_INVALID")
    expired = promo(admin, valid_from="2020-01-01T00:00:00Z", valid_until="2020-02-01T00:00:00Z")
    check("promo expired", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}], "promo_code": expired["code"]}, u2)[:2], 422, "PROMO_CODE_INVALID")
    min_amount = promo(admin, min_order_amount=5000)
    check("promo min amount", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}], "promo_code": min_amount["code"]}, u2)[:2], 422, "PROMO_CODE_MIN_AMOUNT")
    fixed = promo(admin, discount_type="FIXED_AMOUNT", discount_value=5000, max_uses=1)
    o2 = check("fixed promo capped by total", *call("POST", "/orders", {"items": [{"product_id": b["id"], "quantity": 1}], "promo_code": fixed["code"]}, u2)[:2], 201)
    assert o2["total_amount"] == 0 and o2["discount_amount"] == 50, o2
    check("second order within N minutes", *call("POST", "/orders", {"items": [{"product_id": b["id"], "quantity": 1}]}, u2)[:2], 429, "ORDER_LIMIT_EXCEEDED")
    # Сдвигаем последнюю операцию в прошлое, чтобы дойти до проверки активного заказа, не дожидаясь N минут.
    sql(f"UPDATE user_operations SET created_at = created_at - interval '1 day' WHERE user_id = '{o2['user_id']}'")
    check("active order exists", *call("POST", "/orders", {"items": [{"product_id": b["id"], "quantity": 1}]}, u2)[:2], 409, "ORDER_HAS_ACTIVE")

    u3, _ = new_user()
    check("promo exhausted", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}], "promo_code": fixed["code"]}, u3)[:2], 422, "PROMO_CODE_INVALID")
    big = promo(admin, discount_value=80)
    o3 = check("percentage > 70 -> no discount", *call("POST", "/orders", {"items": [{"product_id": a["id"], "quantity": 1}], "promo_code": big["code"]}, u3)[:2], 201)
    assert o3["discount_amount"] == 0 and o3["total_amount"] == 999, o3

    # пересчёт при обновлении: сумма стала меньше min_order_amount -> промокод снимается
    u4, _ = new_user()
    conditional = promo(admin, discount_value=10, min_order_amount=100)
    o4 = check("order with conditional promo", *call("POST", "/orders", {"items": [{"product_id": b["id"], "quantity": 2}], "promo_code": conditional["code"]}, u4)[:2], 201)
    assert o4["discount_amount"] == 10 and o4["promo_code_id"] == conditional["id"], o4
    o4 = check("update below min amount", *call("PUT", f"/orders/{o4['id']}", {"items": [{"product_id": b["id"], "quantity": 1}]}, u4)[:2], 200)
    assert o4["discount_amount"] == 0 and o4["total_amount"] == 50 and o4["promo_code_id"] is None, o4

    print(f"\nall {passed} checks passed")


if __name__ == "__main__":
    main()
