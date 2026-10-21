"""Преобразование ORM-объектов в сгенерированные модели ответов."""

from app.services.money import to_number
from app.tables import Order, Product, PromoCode, User
from marketplace_api.models.order_item_response import OrderItemResponse
from marketplace_api.models.order_response import OrderResponse
from marketplace_api.models.product_response import ProductResponse
from marketplace_api.models.promo_code_response import PromoCodeResponse
from marketplace_api.models.user_response import UserResponse


def user_response(user: User) -> UserResponse:
    return UserResponse(id=str(user.id), email=user.email, role=user.role, created_at=user.created_at)


def product_response(product: Product) -> ProductResponse:
    return ProductResponse(
        id=str(product.id),
        name=product.name,
        description=product.description,
        price=to_number(product.price),
        stock=product.stock,
        category=product.category,
        status=product.status,
        seller_id=str(product.seller_id),
        created_at=product.created_at,
        updated_at=product.updated_at,
    )


def order_response(order: Order) -> OrderResponse:
    return OrderResponse(
        id=str(order.id),
        user_id=str(order.user_id),
        status=order.status,
        promo_code_id=str(order.promo_code_id) if order.promo_code_id else None,
        total_amount=to_number(order.total_amount),
        discount_amount=to_number(order.discount_amount),
        items=[
            OrderItemResponse(
                id=str(item.id),
                product_id=str(item.product_id),
                quantity=item.quantity,
                price_at_order=to_number(item.price_at_order),
            )
            for item in order.items
        ],
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


def promo_code_response(promo: PromoCode) -> PromoCodeResponse:
    return PromoCodeResponse(
        id=str(promo.id),
        code=promo.code,
        discount_type=promo.discount_type,
        discount_value=to_number(promo.discount_value),
        min_order_amount=to_number(promo.min_order_amount),
        max_uses=promo.max_uses,
        current_uses=promo.current_uses,
        valid_from=promo.valid_from,
        valid_until=promo.valid_until,
        active=promo.active,
    )
