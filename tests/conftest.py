from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
  sys.path.insert(0, str(SRC))

from google_adk_redis.redis_memory_session_service import (
  RedisMemorySessionService,
)


def _to_bytes(value: object) -> bytes:
  if isinstance(value, bytes):
    return value
  if isinstance(value, str):
    return value.encode()
  return str(value).encode()


class MockRedis:
  def __init__(self) -> None:
    self._kv: dict[str, bytes] = {}
    self._hashes: dict[str, dict[bytes, bytes]] = {}
    self._expirations: dict[str, int] = {}

  async def get(self, key: str) -> bytes | None:
    return self._kv.get(key)

  async def set(self, key: str, value: object) -> bool:
    self._kv[key] = _to_bytes(value)
    return True

  async def expire(self, key: str, seconds: int) -> bool:
    self._expirations[key] = seconds
    return True

  async def hset(self, key: str, field: object, value: object) -> int:
    field_bytes = _to_bytes(field)
    value_bytes = _to_bytes(value)
    self._hashes.setdefault(key, {})[field_bytes] = value_bytes
    return 1

  async def hgetall(self, key: str) -> dict[bytes, bytes]:
    return dict(self._hashes.get(key, {}))


@pytest.fixture
def session_service() -> RedisMemorySessionService:
  service = RedisMemorySessionService(host="mock", port=0, db=0)
  service.cache = MockRedis()
  return service
