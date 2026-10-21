"""ORM-отображение таблиц из db/migrations.

Перечисления берутся из сгенерированных моделей, поэтому значения в БД и в API совпадают.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.schema import FetchedValue

from marketplace_api.models.discount_type import DiscountType
from marketplace_api.models.order_status import OrderStatus
from marketplace_api.models.product_status import ProductStatus
from marketplace_api.models.user_role import UserRole

Money = Numeric(12, 2)


def _pg_enum(enum_cls: type, name: str) -> ENUM:
    return ENUM(enum_cls, name=name, create_type=False)


class Base(DeclarativeBase):
    # Все временные колонки в БД — TIMESTAMPTZ.
    type_annotation_map = {datetime: DateTime(timezone=True)}
    # Значения server_default / триггеров (created_at, updated_at) читаются через RETURNING.
    __mapper_args__ = {"eager_defaults": True}


class TimestampsMixin:
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    # updated_at обновляет триггер set_updated_at.
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), server_onupdate=FetchedValue())


class User(TimestampsMixin, Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True)
    password_hash: Mapped[str]
    role: Mapped[UserRole] = mapped_column(_pg_enum(UserRole, "user_role"))


class Product(TimestampsMixin, Base):
    __tablename__ = "products"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(String(4000))
    price: Mapped[Decimal] = mapped_column(Money)
    stock: Mapped[int]
    category: Mapped[str] = mapped_column(String(100))
    status: Mapped[ProductStatus] = mapped_column(_pg_enum(ProductStatus, "product_status"))
    seller_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))


class PromoCode(Base):
    __tablename__ = "promo_codes"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    code: Mapped[str] = mapped_column(String(20), unique=True)
    discount_type: Mapped[DiscountType] = mapped_column(_pg_enum(DiscountType, "discount_type"))
    discount_value: Mapped[Decimal] = mapped_column(Money)
    min_order_amount: Mapped[Decimal] = mapped_column(Money)
    max_uses: Mapped[int]
    current_uses: Mapped[int] = mapped_column(default=0)
    valid_from: Mapped[datetime]
    valid_until: Mapped[datetime]
    active: Mapped[bool] = mapped_column(default=True)


class Order(TimestampsMixin, Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    status: Mapped[OrderStatus] = mapped_column(_pg_enum(OrderStatus, "order_status"))
    promo_code_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("promo_codes.id"))
    total_amount: Mapped[Decimal] = mapped_column(Money)
    discount_amount: Mapped[Decimal] = mapped_column(Money, default=Decimal("0"))

    items: Mapped[list["OrderItem"]] = relationship(lazy="selectin", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id"))
    product_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("products.id"))
    quantity: Mapped[int]
    price_at_order: Mapped[Decimal] = mapped_column(Money)


class UserOperation(Base):
    __tablename__ = "user_operations"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    operation_type: Mapped[str] = mapped_column(
        ENUM("CREATE_ORDER", "UPDATE_ORDER", name="operation_type", create_type=False)
    )
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
