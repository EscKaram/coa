import uuid

from app.db import transaction
from app.impl.converters import order_response
from app.services import orders
from marketplace_api.apis.orders_api_base import BaseOrdersApi
from marketplace_api.models.extra_models import TokenModel
from marketplace_api.models.order_create import OrderCreate
from marketplace_api.models.order_response import OrderResponse
from marketplace_api.models.order_status_change import OrderStatusChange
from marketplace_api.models.order_update import OrderUpdate


class OrdersApi(BaseOrdersApi):
    async def create_order(self, order_create: OrderCreate, token: TokenModel) -> OrderResponse:
        async with transaction() as session:
            order = await orders.create_order(session, token, order_create)
        return order_response(order)

    async def get_order(self, id: str, token: TokenModel) -> OrderResponse:
        async with transaction() as session:
            order = await orders.get_order(session, token, uuid.UUID(id))
        return order_response(order)

    async def update_order(self, id: str, order_update: OrderUpdate, token: TokenModel) -> OrderResponse:
        async with transaction() as session:
            order = await orders.update_order(session, token, uuid.UUID(id), order_update)
        return order_response(order)

    async def cancel_order(self, id: str, token: TokenModel) -> OrderResponse:
        async with transaction() as session:
            order = await orders.cancel_order(session, token, uuid.UUID(id))
        return order_response(order)

    async def change_order_status(
        self, id: str, order_status_change: OrderStatusChange, token: TokenModel
    ) -> OrderResponse:
        async with transaction() as session:
            order = await orders.change_status(session, token, uuid.UUID(id), order_status_change.status)
        return order_response(order)
