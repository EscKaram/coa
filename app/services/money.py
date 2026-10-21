"""Денежные значения: в API — number, в БД и расчётах — Decimal с двумя знаками."""

from decimal import ROUND_HALF_UP, Decimal

CENT = Decimal("0.01")
ZERO = Decimal("0.00")
# Предел NUMERIC(12, 2).
MAX_AMOUNT = Decimal("9999999999.99")


def to_money(value: float | int | Decimal) -> Decimal:
    # str() убирает двоичный хвост float: 19.99 -> "19.99", а не 19.989999...
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def to_number(value: Decimal) -> float:
    return float(value)
