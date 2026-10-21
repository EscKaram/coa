"""Промокоды: создание, проверка применимости, расчёт скидки."""

from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.errors import ApiError, validation_error
from app.security import require_role
from app.services.money import CENT, ZERO, to_money
from app.tables import PromoCode
from marketplace_api.models.discount_type import DiscountType
from marketplace_api.models.error_code import ErrorCode
from marketplace_api.models.extra_models import TokenModel
from marketplace_api.models.promo_code_create import PromoCodeCreate
from marketplace_api.models.user_role import UserRole

# Скидка PERCENTAGE больше этой доли от суммы не применяется: заказ оплачивается на 100%.
MAX_PERCENTAGE_DISCOUNT = Decimal("70")


def _as_utc(value: datetime) -> datetime:
    """Дата без часового пояса считается UTC."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


async def create_promo_code(session: AsyncSession, token: TokenModel, data: PromoCodeCreate) -> PromoCode:
    require_role(token, UserRole.SELLER, UserRole.ADMIN)
    discount_value = to_money(data.discount_value)
    valid_from, valid_until = _as_utc(data.valid_from), _as_utc(data.valid_until)

    # Ограничения между полями нельзя выразить в OpenAPI 3.0, проверяем их до записи.
    violations = []
    if data.discount_type == DiscountType.PERCENTAGE and discount_value > 100:
        violations.append({"field": "discount_value", "message": "Percentage discount must be <= 100"})
    if valid_from >= valid_until:
        violations.append({"field": "valid_until", "message": "valid_until must be later than valid_from"})
    if violations:
        raise validation_error("Promo code validation failed", violations)

    promo = PromoCode(
        code=data.code,
        discount_type=data.discount_type,
        discount_value=discount_value,
        min_order_amount=to_money(data.min_order_amount if data.min_order_amount is not None else 0),
        max_uses=data.max_uses,
        current_uses=0,
        valid_from=valid_from,
        valid_until=valid_until,
        active=data.active if data.active is not None else True,
    )
    session.add(promo)
    try:
        await session.flush()
    except IntegrityError:
        raise ApiError(
            409, ErrorCode.PROMO_CODE_ALREADY_EXISTS, f"Promo code {data.code} already exists", {"code": data.code}
        ) from None
    return promo


def promo_invalid(code: str, reason: str) -> ApiError:
    return ApiError(
        422, ErrorCode.PROMO_CODE_INVALID, f"Promo code {code} cannot be applied", {"code": code, "reason": reason}
    )


async def lock_by_code(session: AsyncSession, code: str) -> PromoCode | None:
    query = select(PromoCode).where(PromoCode.code == code).with_for_update()
    return (await session.execute(query)).scalar_one_or_none()


async def lock_by_id(session: AsyncSession, promo_id) -> PromoCode:
    query = select(PromoCode).where(PromoCode.id == promo_id).with_for_update()
    return (await session.execute(query)).scalar_one()


def unusable_reason(promo: PromoCode, now: datetime, *, needs_free_use: bool) -> str | None:
    """Причина, по которой промокод нельзя применить, или None.

    needs_free_use=False при пересчёте уже применённого промокода: его использование уже учтено в current_uses.
    """
    if not promo.active:
        return "INACTIVE"
    if now < promo.valid_from:
        return "NOT_STARTED"
    if now > promo.valid_until:
        return "EXPIRED"
    if needs_free_use and promo.current_uses >= promo.max_uses:
        return "EXHAUSTED"
    return None


def calculate_discount(promo: PromoCode, subtotal: Decimal) -> Decimal:
    if promo.discount_type == DiscountType.PERCENTAGE:
        if promo.discount_value > MAX_PERCENTAGE_DISCOUNT:
            return ZERO
        return (subtotal * promo.discount_value / 100).quantize(CENT, rounding=ROUND_HALF_UP)
    return min(promo.discount_value, subtotal)
