from app.db import transaction
from app.impl.converters import user_response
from app.services import auth
from marketplace_api.apis.auth_api_base import BaseAuthApi
from marketplace_api.models.login_request import LoginRequest
from marketplace_api.models.refresh_request import RefreshRequest
from marketplace_api.models.register_request import RegisterRequest
from marketplace_api.models.token_pair import TokenPair
from marketplace_api.models.user_response import UserResponse


class AuthApi(BaseAuthApi):
    async def register(self, register_request: RegisterRequest) -> UserResponse:
        async with transaction() as session:
            user = await auth.register(session, register_request)
        return user_response(user)

    async def login(self, login_request: LoginRequest) -> TokenPair:
        async with transaction() as session:
            return await auth.login(session, login_request)

    async def refresh(self, refresh_request: RefreshRequest) -> TokenPair:
        async with transaction() as session:
            return await auth.refresh(session, refresh_request.refresh_token)
