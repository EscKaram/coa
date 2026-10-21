"""Пароли, JWT-токены, аутентификация запросов и проверка ролей."""

import base64
import hashlib
import hmac
import os
import uuid
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Request

from app.config import settings
from app.errors import ApiError, access_denied
from marketplace_api.models.error_code import ErrorCode
from marketplace_api.models.extra_models import TokenModel
from marketplace_api.models.user_role import UserRole

JWT_ALGORITHM = "HS256"
ACCESS = "access"
REFRESH = "refresh"

_PBKDF2_ITERATIONS = 200_000


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _PBKDF2_ITERATIONS)
    return "$".join(
        ["pbkdf2_sha256", str(_PBKDF2_ITERATIONS), base64.b64encode(salt).decode(), base64.b64encode(digest).decode()]
    )


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations, salt_b64, digest_b64 = password_hash.split("$")
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.b64decode(salt_b64), int(iterations))
    return hmac.compare_digest(digest, base64.b64decode(digest_b64))


def _encode(claims: dict, ttl: timedelta) -> str:
    now = datetime.now(timezone.utc)
    payload = {**claims, "iat": now, "exp": now + ttl, "jti": str(uuid.uuid4())}
    return jwt.encode(payload, settings.jwt_secret, algorithm=JWT_ALGORITHM)


def access_token_ttl() -> timedelta:
    return timedelta(minutes=settings.access_token_ttl_minutes)


def create_access_token(user_id: uuid.UUID, role: UserRole) -> str:
    return _encode({"sub": str(user_id), "role": role.value, "type": ACCESS}, access_token_ttl())


def create_refresh_token(user_id: uuid.UUID) -> str:
    return _encode({"sub": str(user_id), "type": REFRESH}, timedelta(days=settings.refresh_token_ttl_days))


def _decode(token: str, expected_type: str) -> dict:
    """Проверяет подпись, срок действия и тип токена. Ошибки PyJWT пробрасываются вызывающему."""
    claims = jwt.decode(
        token, settings.jwt_secret, algorithms=[JWT_ALGORITHM], options={"require": ["sub", "exp", "type"]}
    )
    if claims.get("type") != expected_type:
        raise jwt.InvalidTokenError(f"expected {expected_type} token")
    uuid.UUID(claims["sub"])
    return claims


def decode_refresh_token(token: str) -> uuid.UUID:
    try:
        return uuid.UUID(_decode(token, REFRESH)["sub"])
    except (jwt.PyJWTError, ValueError):
        raise ApiError(401, ErrorCode.REFRESH_TOKEN_INVALID, "Refresh token is invalid or expired") from None


def authenticate(request: Request) -> TokenModel:
    """Замена сгенерированной зависимости get_token_bearerAuth: проверяет access-токен.

    ID пользователя сохраняется в request.state для логирования запроса.
    """
    scheme, _, token = request.headers.get("Authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise ApiError(401, ErrorCode.TOKEN_INVALID, "Missing or malformed Authorization header")
    try:
        claims = _decode(token, ACCESS)
        role = UserRole(claims["role"])
    except jwt.ExpiredSignatureError:
        raise ApiError(401, ErrorCode.TOKEN_EXPIRED, "Access token has expired") from None
    except (jwt.PyJWTError, KeyError, ValueError):
        raise ApiError(401, ErrorCode.TOKEN_INVALID, "Access token is invalid") from None
    request.state.user_id = claims["sub"]
    return TokenModel(sub=claims["sub"], role=role.value)


def require_role(token: TokenModel, *roles: UserRole) -> None:
    if token.role not in {r.value for r in roles}:
        raise access_denied(f"Role {token.role} is not allowed to perform this operation")


def user_id_of(token: TokenModel) -> uuid.UUID:
    return uuid.UUID(token.sub)


def is_admin(token: TokenModel) -> bool:
    return token.role == UserRole.ADMIN.value
