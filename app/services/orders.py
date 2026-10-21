"""Бизнес-логика заказов: создание, обновление, отмена, переходы состояний.

Каждая операция выполняется в одной транзакции (см. app.db.transaction). Строки, которые
меняются, блокируются через SELECT ... FOR UPDATE:
- строка пользователя — сериализует операции одного пользователя (лимит частоты, активный заказ);
- строки товаров — в порядке id, чтобы параллельные заказы не взаимоблокировались;
- строка промокода — чтобы current_uses не превысил max_uses.
"""

import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.errors import ApiError, validation_error
from app.security import is_admin, require_role, user_id_of
from app.services import promo_codes
from app.services.money import MAX_AMOUNT, ZERO
from app.services.products import product_not_found
from app.tables import Order, OrderItem, Product, User, UserOperation
from marketplace_api.models.error_code import ErrorCode
from marketplace_api.models.extra_models import TokenModel
from marketplace_api.models.order_create import OrderCreate
from marketplace_api.models.order_item_request import OrderItemRequest
from marketplace_api.models.order_status import OrderStatus
from marketplace_api.models.order_update import OrderUpdate
from marketplace_api.models.product_status import ProductStatus
from marketplace_api.models.user_role import UserRole

CREATE_ORDER = "CREATE_ORDER"
UPDATE_ORDER = "UPDATE_ORDER"

ACTIVE_STATUSES = (OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING)

# CREATED → PAYMENT_PENDING → PAID → SHIPPED → COMPLETED; отмена из CREATED и PAYMENT_PENDING.
TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.CREATED: frozenset({OrderStatus.PAYMENT_PENDING, OrderStatus.CANCELED}),
    OrderStatus.PAYMENT_PENDING: frozenset({OrderStatus.PAID, OrderStatus.CANCELED}),
    OrderStatus.PAID: frozenset({OrderStatus.SHIPPED}),
    OrderStatus.SHIPPED: frozenset({OrderStatus.COMPLETED}),
    OrderStatus.COMPLETED: frozenset(),
    OrderStatus.CANCELED: frozenset(),
}


def invalid_transition(order: Order, target: OrderStatus | str) -> ApiError:
    target = target.value if isinstance(target, OrderStatus) else target
    return ApiError(
        409,
        ErrorCode.INVALID_STATE_TRANSITION,
        f"Order cannot go from {order.status.value} to {target}",
        {
            "order_id": str(order.id),
            "current_status": order.status.value,
            "target_status": target,
            "allowed": sorted(s.value for s in TRANSITIONS[order.status]),
        },
    )


# ---------- общие шаги ----------


async def _lock_user(session: AsyncSession, user_id: uuid.UUID) -> None:
    await session.execute(select(User.id).where(User.id == user_id).with_for_update())


async def _check_rate_limit(session: AsyncSession, user_id: uuid.UUID, operation_type: str) -> None:
    limit = timedelta(minutes=settings.order_rate_limit_minutes)
    if not limit:
        return
    row = (
        await session.execute(
            select(UserOperation.created_at, func.now())
            .where(UserOperation.user_id == user_id, UserOperation.operation_type == operation_type)
            .order_by(UserOperation.created_at.desc())
            .limit(1)
        )
    ).first()
    if row is None:
        return
    last_at, now = row
    elapsed = now - last_at
    if elapsed < limit:
        retry_after = int((limit - elapsed).total_seconds()) + 1
        raise ApiError(
            429,
            ErrorCode.ORDER_LIMIT_EXCEEDED,
            f"{operation_type} is allowed once per {settings.order_rate_limit_minutes} min",
            {"operation_type": operation_type, "retry_after_seconds": retry_after},
        )


def _record_operation(session: AsyncSession, user_id: uuid.UUID, operation_type: str) -> None:
    session.add(UserOperation(user_id=user_id, operation_type=operation_type))


async def _lock_products(session: AsyncSession, product_ids: set[uuid.UUID]) -> dict[uuid.UUID, Product]:
    rows = await session.execute(
        select(Product).where(Product.id.in_(product_ids)).order_by(Product.id).with_for_update()
    )
    return {p.id: p for p in rows.scalars()}


def _parse_items(items: list[OrderItemRequest]) -> list[tuple[uuid.UUID, int]]:
    # Формат UUID уже проверен по pattern из OpenAPI.
    return [(uuid.UUID(item.product_id), item.quantity) for item in items]


def _check_catalog(lines: list[tuple[uuid.UUID, int]], products: dict[uuid.UUID, Product]) -> None:
    for product_id, _ in lines:
        product = products.get(product_id)
        if product is None:
            raise product_not_found(product_id)
        if product.status != ProductStatus.ACTIVE:
            raise ApiError(
                409,
                ErrorCode.PRODUCT_INACTIVE,
                f"Product {product_id} is not available for ordering",
                {"product_id": str(product_id), "status": product.status.value},
            )


def _reserve_stock(lines: list[tuple[uuid.UUID, int]], products: dict[uuid.UUID, Product]) -> None:
    """Проверяет остатки по всем позициям и только затем списывает их."""
    requested: dict[uuid.UUID, int] = defaultdict(int)
    for product_id, quantity in lines:
        requested[product_id] += quantity

    shortages = [
        {"product_id": str(pid), "requested": qty, "available": products[pid].stock}
        for pid, qty in requested.items()
        if products[pid].stock < qty
    ]
    if shortages:
        raise ApiError(409, ErrorCode.INSUFFICIENT_STOCK, "Not enough stock for some items", {"items": shortages})

    for pid, qty in requested.items():
        products[pid].stock -= qty


def _build_items(lines: list[tuple[uuid.UUID, int]], products: dict[uuid.UUID, Product]) -> list[OrderItem]:
    # price_at_order — снимок цены: дальнейшие изменения товара заказ не затрагивают.
    return [OrderItem(product_id=pid, quantity=qty, price_at_order=products[pid].price) for pid, qty in lines]


def _subtotal(items: list[OrderItem]) -> Decimal:
    subtotal = sum((item.price_at_order * item.quantity for item in items), ZERO)
    if subtotal > MAX_AMOUNT:
        raise validation_error(
            "Order total exceeds the maximum amount",
            [{"field": "items", "message": f"Order total must not exceed {MAX_AMOUNT}"}],
        )
    return subtotal


def _return_stock(order: Order, products: dict[uuid.UUID, Product]) -> None:
    for item in order.items:
        products[item.product_id].stock += item.quantity


async def _get_order_for_update(session: AsyncSession, order_id: uuid.UUID) -> Order:
    order = (await session.execute(select(Order).where(Order.id == order_id).with_for_update())).scalar_one_or_none()
    if order is None:
        raise ApiError(404, ErrorCode.ORDER_NOT_FOUND, f"Order {order_id} not found", {"order_id": str(order_id)})
    return order


def _check_owner(token: TokenModel, order: Order) -> None:
    if not is_admin(token) and order.user_id != user_id_of(token):
        raise ApiError(403, ErrorCode.ORDER_OWNERSHIP_VIOLATION, "Order belongs to another user")


# ---------- операции ----------


async def create_order(session: AsyncSession, token: TokenModel, data: OrderCreate) -> Order:
    require_role(token, UserRole.USER, UserRole.ADMIN)
    user_id = user_id_of(token)
    await _lock_user(session, user_id)

    # 1. Ограничение частоты создания.
    await _check_rate_limit(session, user_id, CREATE_ORDER)

    # 2. Активный заказ.
    active = await session.execute(
        select(Order.id).where(Order.user_id == user_id, Order.status.in_(ACTIVE_STATUSES)).limit(1)
    )
    active_id = active.scalar_one_or_none()
    if active_id is not None:
        raise ApiError(
            409, ErrorCode.ORDER_HAS_ACTIVE, "User already has an active order", {"order_id": str(active_id)}
        )

    # 3–5. Каталог, остатки, резервирование.
    lines = _parse_items(data.items)
    products = await _lock_products(session, {pid for pid, _ in lines})
    _check_catalog(lines, products)
    _reserve_stock(lines, products)

    # 6–7. Снимок цен и расчёт стоимости.
    items = _build_items(lines, products)
    subtotal = _subtotal(items)
    discount = ZERO
    promo_id = None
    if data.promo_code is not None:
        promo = await promo_codes.lock_by_code(session, data.promo_code)
        if promo is None:
            raise promo_codes.promo_invalid(data.promo_code, "NOT_FOUND")
        reason = promo_codes.unusable_reason(promo, datetime.now(timezone.utc), needs_free_use=True)
        if reason is not None:
            raise promo_codes.promo_invalid(promo.code, reason)
        if subtotal < promo.min_order_amount:
            raise ApiError(
                422,
                ErrorCode.PROMO_CODE_MIN_AMOUNT,
                f"Order total is below the minimum for promo code {promo.code}",
                {"code": promo.code, "order_amount": str(subtotal), "min_order_amount": str(promo.min_order_amount)},
            )
        discount = promo_codes.calculate_discount(promo, subtotal)
        promo.current_uses += 1
        promo_id = promo.id

    # 8. Заказ и фиксация операции.
    order = Order(
        user_id=user_id,
        status=OrderStatus.CREATED,
        promo_code_id=promo_id,
        total_amount=subtotal - discount,
        discount_amount=discount,
        items=items,
    )
    session.add(order)
    _record_operation(session, user_id, CREATE_ORDER)
    await session.flush()
    return order


async def get_order(session: AsyncSession, token: TokenModel, order_id: uuid.UUID) -> Order:
    require_role(token, UserRole.USER, UserRole.ADMIN)
    order = (await session.execute(select(Order).where(Order.id == order_id))).scalar_one_or_none()
    if order is None:
        raise ApiError(404, ErrorCode.ORDER_NOT_FOUND, f"Order {order_id} not found", {"order_id": str(order_id)})
    _check_owner(token, order)
    return order


async def update_order(session: AsyncSession, token: TokenModel, order_id: uuid.UUID, data: OrderUpdate) -> Order:
    require_role(token, UserRole.USER, UserRole.ADMIN)
    order = await _get_order_for_update(session, order_id)

    # 1. Владелец.
    _check_owner(token, order)

    # 2. Состояние: менять позиции можно только в CREATED.
    if order.status != OrderStatus.CREATED:
        raise ApiError(
            409,
            ErrorCode.INVALID_STATE_TRANSITION,
            f"Order in status {order.status.value} cannot be updated",
            {"order_id": str(order.id), "current_status": order.status.value, "required_status": "CREATED"},
        )

    # 3. Ограничение частоты обновления (по пользователю, который выполняет запрос).
    actor_id = user_id_of(token)
    await _lock_user(session, actor_id)
    await _check_rate_limit(session, actor_id, UPDATE_ORDER)

    # 4. Возврат остатков старых позиций; 5. проверка и резервирование новых.
    lines = _parse_items(data.items)
    products = await _lock_products(session, {i.product_id for i in order.items} | {pid for pid, _ in lines})
    _return_stock(order, products)
    _check_catalog(lines, products)
    _reserve_stock(lines, products)
    order.items = _build_items(lines, products)

    # 6. Пересчёт стоимости и промокода.
    subtotal = _subtotal(order.items)
    discount = ZERO
    if order.promo_code_id is not None:
        promo = await promo_codes.lock_by_id(session, order.promo_code_id)
        still_valid = promo_codes.unusable_reason(promo, datetime.now(timezone.utc), needs_free_use=False) is None
        if still_valid and subtotal >= promo.min_order_amount:
            discount = promo_codes.calculate_discount(promo, subtotal)
        else:
            # Промокод больше не применим: снимаем скидку и возвращаем использование.
            order.promo_code_id = None
            promo.current_uses -= 1
    order.total_amount = subtotal - discount
    order.discount_amount = discount

    # 7. Фиксация операции.
    _record_operation(session, actor_id, UPDATE_ORDER)
    await session.flush()
    return order


async def _cancel(session: AsyncSession, order: Order) -> None:
    if order.status not in ACTIVE_STATUSES:
        raise invalid_transition(order, OrderStatus.CANCELED)
    products = await _lock_products(session, {i.product_id for i in order.items})
    _return_stock(order, products)
    if order.promo_code_id is not None:
        promo = await promo_codes.lock_by_id(session, order.promo_code_id)
        promo.current_uses -= 1
    order.status = OrderStatus.CANCELED


async def cancel_order(session: AsyncSession, token: TokenModel, order_id: uuid.UUID) -> Order:
    require_role(token, UserRole.USER, UserRole.ADMIN)
    order = await _get_order_for_update(session, order_id)
    _check_owner(token, order)
    await _cancel(session, order)
    await session.flush()
    return order


async def change_status(session: AsyncSession, token: TokenModel, order_id: uuid.UUID, target: OrderStatus) -> Order:
    require_role(token, UserRole.USER, UserRole.ADMIN)
    order = await _get_order_for_update(session, order_id)
    _check_owner(token, order)
    if target == OrderStatus.CANCELED:
        await _cancel(session, order)
    elif target in TRANSITIONS[order.status]:
        order.status = target
    else:
        raise invalid_transition(order, target)
    await session.flush()
    return order
