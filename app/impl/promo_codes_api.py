from app.db import transaction
from app.impl.converters import promo_code_response
from app.services import promo_codes
from marketplace_api.apis.promo_codes_api_base import BasePromoCodesApi
from marketplace_api.models.extra_models import TokenModel
from marketplace_api.models.promo_code_create import PromoCodeCreate
from marketplace_api.models.promo_code_response import PromoCodeResponse


class PromoCodesApi(BasePromoCodesApi):
    async def create_promo_code(self, promo_code_create: PromoCodeCreate, token: TokenModel) -> PromoCodeResponse:
        async with transaction() as session:
            promo = await promo_codes.create_promo_code(session, token, promo_code_create)
        return promo_code_response(promo)
