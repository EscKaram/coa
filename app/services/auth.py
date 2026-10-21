"""Регистрация, вход и обновление токенов."""

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.errors import ApiError
from app.security import (
    access_token_ttl,
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    hash_password,
    verify_password,
)
from app.tables import User
from marketplace_api.models.error_code import ErrorCode
from marketplace_api.models.login_request import LoginRequest
from marketplace_api.models.register_request import RegisterRequest
from marketplace_api.models.token_pair import TokenPair
from marketplace_api.models.user_role import UserRole


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _token_pair(user: User) -> TokenPair:
    return TokenPair(
        access_token=create_access_token(user.id, user.role),
        refresh_token=create_refresh_token(user.id),
        token_type="Bearer",
        expires_in=int(access_token_ttl().total_seconds()),
    )


async def register(session: AsyncSession, data: RegisterRequest) -> User:
    email = _normalize_email(data.email)
    user = User(email=email, password_hash=hash_password(data.password), role=UserRole(data.role or "USER"))
    session.add(user)
    try:
        await session.flush()
    except IntegrityError:
        raise ApiError(409, ErrorCode.USER_ALREADY_EXISTS, f"User {email} already exists") from None
    return user


async def login(session: AsyncSession, data: LoginRequest) -> TokenPair:
    user = (await session.execute(select(User).where(User.email == _normalize_email(data.email)))).scalar_one_or_none()
    if user is None or not verify_password(data.password, user.password_hash):
        raise ApiError(401, ErrorCode.INVALID_CREDENTIALS, "Invalid email or password")
    return _token_pair(user)


async def refresh(session: AsyncSession, refresh_token: str) -> TokenPair:
    user_id = decode_refresh_token(refresh_token)
    user = await session.get(User, user_id)
    if user is None:
        raise ApiError(401, ErrorCode.REFRESH_TOKEN_INVALID, "Refresh token is invalid or expired")
    return _token_pair(user)


async def ensure_admin(session: AsyncSession, email: str, password: str) -> bool:
    """Создаёт администратора при старте, если его ещё нет. Возвращает True, если пользователь создан."""
    email = _normalize_email(email)
    if (await session.execute(select(User.id).where(User.email == email))).first() is not None:
        return False
    session.add(User(email=email, password_hash=hash_password(password), role=UserRole.ADMIN))
    await session.flush()
    return True
