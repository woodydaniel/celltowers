"""
JWT authentication for the Cell Tower Search API.
Credentials are validated server-side only — never exposed to the client.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel

# Secret key — override via JWT_SECRET env var in production
SECRET_KEY = os.getenv("JWT_SECRET", "celltowers-jwt-secret-2026-change-in-prod")
ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 12

# ---------------------------------------------------------------------------
# Authorised users — server-side only, never sent to the browser
# ---------------------------------------------------------------------------
_VALID_USERS: dict[str, str] = {
    "mendy@migdaltowers.com": "celltowers2026!",
}


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


def create_access_token(email: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)
    payload = {"sub": email, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def validate_login(email: str, password: str) -> bool:
    return _VALID_USERS.get(email.lower().strip()) == password


# ---------------------------------------------------------------------------
# FastAPI dependency — attach to any route that requires auth
# ---------------------------------------------------------------------------
_bearer = HTTPBearer(auto_error=True)


def require_auth(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> str:
    """Validates the Bearer JWT and returns the authenticated email."""
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        email: str | None = payload.get("sub")
        if not email:
            raise ValueError("missing sub")
        return email
    except (JWTError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
