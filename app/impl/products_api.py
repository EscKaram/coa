import math
import uuid

from app.db import transaction
from app.impl.converters import product_response
from app.services import products
from marketplace_api.apis.products_api_base import BaseProductsApi
from marketplace_api.models.extra_models import TokenModel
from marketplace_api.models.product_create import ProductCreate
from marketplace_api.models.product_page import ProductPage
from marketplace_api.models.product_response import ProductResponse
from marketplace_api.models.product_status import ProductStatus
from marketplace_api.models.product_update import ProductUpdate


class ProductsApi(BaseProductsApi):
    async def create_product(self, product_create: ProductCreate, token: TokenModel) -> ProductResponse:
        async with transaction() as session:
            product = await products.create_product(session, token, product_create)
        return product_response(product)

    async def get_product(self, id: str, token: TokenModel) -> ProductResponse:
        async with transaction() as session:
            product = await products.get_product(session, uuid.UUID(id))
        return product_response(product)

    async def list_products(
        self, page: int | None, size: int | None, status: ProductStatus | None, category: str | None, token: TokenModel
    ) -> ProductPage:
        page = page if page is not None else 0
        size = size if size is not None else 20
        async with transaction() as session:
            items, total = await products.list_products(session, page, size, status, category)
        return ProductPage(
            items=[product_response(p) for p in items],
            totalElements=total,
            page=page,
            size=size,
            totalPages=math.ceil(total / size),
        )

    async def update_product(self, id: str, product_update: ProductUpdate, token: TokenModel) -> ProductResponse:
        async with transaction() as session:
            product = await products.update_product(session, token, uuid.UUID(id), product_update)
        return product_response(product)

    async def delete_product(self, id: str, token: TokenModel) -> ProductResponse:
        async with transaction() as session:
            product = await products.archive_product(session, token, uuid.UUID(id))
        return product_response(product)
