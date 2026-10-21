"""CRUD товаров с учётом ролей: SELLER управляет только своими товарами, ADMIN — любыми."""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.errors import ApiError, access_denied
from app.security import is_admin, require_role, user_id_of
from app.services.money import to_money
from app.tables import Product
from marketplace_api.models.error_code import ErrorCode
from marketplace_api.models.extra_models import TokenModel
from marketplace_api.models.product_create import ProductCreate
from marketplace_api.models.product_status import ProductStatus
from marketplace_api.models.product_update import ProductUpdate
from marketplace_api.models.user_role import UserRole


def product_not_found(product_id: uuid.UUID) -> ApiError:
    return ApiError(404, ErrorCode.PRODUCT_NOT_FOUND, f"Product {product_id} not found", {"product_id": str(product_id)})


async def _get(session: AsyncSession, product_id: uuid.UUID, *, for_update: bool = False) -> Product:
    query = select(Product).where(Product.id == product_id)
    if for_update:
        query = query.with_for_update()
    product = (await session.execute(query)).scalar_one_or_none()
    if product is None:
        raise product_not_found(product_id)
    return product


def _check_can_manage(token: TokenModel, product: Product) -> None:
    if not is_admin(token) and product.seller_id != user_id_of(token):
        raise access_denied("Sellers can manage only their own products")


async def create_product(session: AsyncSession, token: TokenModel, data: ProductCreate) -> Product:
    require_role(token, UserRole.SELLER, UserRole.ADMIN)
    product = Product(
        name=data.name,
        description=data.description,
        price=to_money(data.price),
        stock=data.stock,
        category=data.category,
        status=data.status,
        seller_id=user_id_of(token),
    )
    session.add(product)
    await session.flush()
    return product


async def get_product(session: AsyncSession, product_id: uuid.UUID) -> Product:
    return await _get(session, product_id)


async def list_products(
    session: AsyncSession, page: int, size: int, status: ProductStatus | None, category: str | None
) -> tuple[list[Product], int]:
    conditions = []
    if status is not None:
        conditions.append(Product.status == status)
    if category is not None:
        conditions.append(Product.category == category)

    total = (await session.execute(select(func.count()).select_from(Product).where(*conditions))).scalar_one()
    rows = await session.execute(
        select(Product)
        .where(*conditions)
        .order_by(Product.created_at.desc(), Product.id)
        .offset(page * size)
        .limit(size)
    )
    return list(rows.scalars()), total


async def update_product(
    session: AsyncSession, token: TokenModel, product_id: uuid.UUID, data: ProductUpdate
) -> Product:
    require_role(token, UserRole.SELLER, UserRole.ADMIN)
    product = await _get(session, product_id, for_update=True)
    _check_can_manage(token, product)
    product.name = data.name
    product.description = data.description
    product.price = to_money(data.price)
    product.stock = data.stock
    product.category = data.category
    product.status = data.status
    await session.flush()
    return product


async def archive_product(session: AsyncSession, token: TokenModel, product_id: uuid.UUID) -> Product:
    """Мягкое удаление: запись остаётся, статус меняется на ARCHIVED."""
    require_role(token, UserRole.SELLER, UserRole.ADMIN)
    product = await _get(session, product_id, for_update=True)
    _check_can_manage(token, product)
    product.status = ProductStatus.ARCHIVED
    await session.flush()
    return product
