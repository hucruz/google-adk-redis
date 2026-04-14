from __future__ import annotations

import asyncio
import fnmatch
import sys
from pathlib import Path

import pytest
from redis.exceptions import WatchError

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


class MockPipeline:
  def __init__(self, client: "MockRedis", *, transaction: bool = True) -> None:
    self._client = client
    self._transaction = transaction
    self._commands: list[tuple[str, tuple[object, ...]]] = []
    self._watched_versions: dict[str, int] = {}
    self._in_multi = False

  async def __aenter__(self) -> "MockPipeline":
    return self

  async def __aexit__(self, exc_type, exc_value, traceback) -> None:
    del exc_type, exc_value, traceback
    self._commands.clear()
    self._watched_versions.clear()
    self._in_multi = False

  async def watch(self, *keys: str) -> None:
    await asyncio.sleep(0)
    for key in keys:
      self._watched_versions[key] = self._client.version_of(key)

  async def get(self, key: str):
    if self._transaction and not self._in_multi:
      return await self._client.get(key)
    self._commands.append(("get", (key,)))
    return self

  def multi(self) -> None:
    self._commands.clear()
    self._in_multi = True

  def set(self, key: str, value: object) -> None:
    self._commands.append(("set", (key, value)))

  def expire(self, key: str, seconds: int) -> None:
    self._commands.append(("expire", (key, seconds)))

  def hset(self, key: str, field: object, value: object) -> None:
    self._commands.append(("hset", (key, field, value)))

  async def execute(self) -> list[object]:
    await asyncio.sleep(0)
    self._check_watches()
    results: list[object] = []
    for command, args in self._commands:
      method = getattr(self._client, command)
      results.append(await method(*args))
    self._commands.clear()
    self._watched_versions.clear()
    self._in_multi = False
    return results

  def _check_watches(self) -> None:
    for key, watched_version in self._watched_versions.items():
      if self._client.version_of(key) != watched_version:
        raise WatchError("Watched key changed during transaction")


class MockRedis:
  def __init__(self) -> None:
    self._kv: dict[str, bytes] = {}
    self._hashes: dict[str, dict[bytes, bytes]] = {}
    self._expirations: dict[str, int] = {}
    self._versions: dict[str, int] = {}

  def version_of(self, key: str) -> int:
    return self._versions.get(key, 0)

  def _touch(self, key: str) -> None:
    self._versions[key] = self.version_of(key) + 1

  async def get(self, key: str) -> bytes | None:
    return self._kv.get(key)

  async def set(self, key: str, value: object) -> bool:
    self._kv[key] = _to_bytes(value)
    self._touch(key)
    return True

  async def expire(self, key: str, seconds: int) -> bool:
    self._expirations[key] = seconds
    self._touch(key)
    return True

  async def delete(self, *keys: str) -> int:
    deleted = 0
    for key in keys:
      if key in self._kv:
        del self._kv[key]
        deleted += 1
      if key in self._expirations:
        del self._expirations[key]
      self._touch(key)
    return deleted

  async def hset(self, key: str, field: object, value: object) -> int:
    field_bytes = _to_bytes(field)
    value_bytes = _to_bytes(value)
    self._hashes.setdefault(key, {})[field_bytes] = value_bytes
    self._touch(key)
    return 1

  async def hgetall(self, key: str) -> dict[bytes, bytes]:
    return dict(self._hashes.get(key, {}))

  async def keys(self, pattern: str) -> list[bytes]:
    return [
        key.encode()
        for key in self._kv.keys()
        if fnmatch.fnmatch(key, pattern)
    ]

  async def scan_iter(self, match: str | None = None):
    pattern = match or "*"
    for key in list(self._kv.keys()):
      if fnmatch.fnmatch(key, pattern):
        yield key.encode()

  def pipeline(self, transaction: bool = True) -> MockPipeline:
    return MockPipeline(self, transaction=transaction)


@pytest.fixture
def session_service() -> RedisMemorySessionService:
  return RedisMemorySessionService(
      host="mock",
      port=0,
      db=0,
      cache=MockRedis(),
  )


@pytest.fixture(autouse=True)
def clear_shared_caches():
  RedisMemorySessionService._shared_caches.clear()
  RedisMemorySessionService._session_write_locks.clear()
  yield
  RedisMemorySessionService._shared_caches.clear()
  RedisMemorySessionService._session_write_locks.clear()
