# Copyright 2025 BloodBoy21
# Copyright 2025 hucruz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Attribution:
# - Original implementation by BloodBoy21, with contributions by hucruz:
#   https://github.com/BloodBoy21/nerds-adk-python/tree/feat-redis-session
from __future__ import annotations

import asyncio
import base64
import copy
import datetime
from decimal import Decimal
import json
import logging
import math
import random
from threading import Lock
import time
from typing import Any
from typing import ClassVar
from typing import Optional
import uuid
import weakref

import redis.asyncio as redis
from redis.exceptions import WatchError
from typing_extensions import override

from google.adk.errors.already_exists_error import AlreadyExistsError
from google.adk.events.event import Event
from google.adk.sessions import _session_util
from google.adk.sessions.base_session_service import BaseSessionService
from google.adk.sessions.base_session_service import GetSessionConfig
from google.adk.sessions.base_session_service import ListSessionsResponse
from google.adk.sessions.session import Session
from google.adk.sessions.state import State

logger = logging.getLogger("google_adk_redis." + __name__)

SESSION_PREFIX = "session:"
DEFAULT_EXPIRATION = 60 * 60  # 1 hour
MAX_TRANSACTION_RETRIES = 8
DEFAULT_TRANSACTION_RETRY_BASE_DELAY = 0.01
DEFAULT_TRANSACTION_RETRY_MAX_DELAY = 0.25


def _json_serializer(obj):
  """Fallback serializer to handle non-JSON-compatible types."""
  if isinstance(obj, set):
    return list(obj)
  if isinstance(obj, bytes):
    try:
      return base64.b64encode(obj).decode("ascii")
    except Exception:
      return repr(obj)
  if isinstance(obj, (datetime.datetime, datetime.date)):
    return obj.isoformat()
  if isinstance(obj, uuid.UUID):
    return str(obj)
  if isinstance(obj, Decimal):
    return float(obj)
  if isinstance(obj, float):
    if math.isnan(obj):
      return "NaN"
    if math.isinf(obj):
      return "Infinity" if obj > 0 else "-Infinity"
  return str(obj)


def _restore_bytes(obj):
  if isinstance(obj, dict):
    return {k: _restore_bytes(v) for k, v in obj.items()}
  elif isinstance(obj, list):
    return [_restore_bytes(v) for v in obj]
  elif isinstance(obj, str):
    try:
      # intenta decodificar base64
      data = base64.b64decode(obj, validate=True)
      return data
    except Exception:
      return obj
  return obj


def _session_to_dict(session: Session) -> dict[str, Any]:
  if hasattr(session, "to_dict"):
    return session.to_dict()
  if hasattr(session, "model_dump"):
    return session.model_dump(
        mode="json",
        by_alias=False,
        exclude_none=True,
    )
  return session.dict(by_alias=False, exclude_none=True)


def _session_from_dict(data: dict[str, Any]) -> Session:
  if hasattr(Session, "from_dict"):
    return Session.from_dict(data)
  if hasattr(Session, "model_validate"):
    return Session.model_validate(data)
  return Session.parse_obj(data)


class RedisMemorySessionService(BaseSessionService):
  """A Redis-backed implementation of the session service."""

  _shared_caches: ClassVar[dict[tuple[str, ...], redis.Redis]] = {}
  _shared_caches_lock: ClassVar[Lock] = Lock()
  _session_write_locks: ClassVar[
      weakref.WeakKeyDictionary[Any, dict[tuple[int, str], asyncio.Lock]]
  ] = weakref.WeakKeyDictionary()
  _session_write_locks_lock: ClassVar[Lock] = Lock()

  def __init__(
      self,
      host="localhost",
      port=6379,
      db=0,
      uri=None,
      expire=DEFAULT_EXPIRATION,
      max_transaction_retries: int = MAX_TRANSACTION_RETRIES,
      transaction_retry_base_delay: float = (
          DEFAULT_TRANSACTION_RETRY_BASE_DELAY
      ),
      transaction_retry_max_delay: float = DEFAULT_TRANSACTION_RETRY_MAX_DELAY,
      cache: Optional[Any] = None,
      share_cache: bool = True,
  ):
    self.host = host
    self.port = port
    self.db = db
    self.uri = uri
    self.expire = expire
    self.max_transaction_retries = max(1, max_transaction_retries)
    self.transaction_retry_base_delay = max(0.0, transaction_retry_base_delay)
    self.transaction_retry_max_delay = max(
        self.transaction_retry_base_delay,
        transaction_retry_max_delay,
    )

    if cache is not None:
      self.cache = cache
    elif share_cache:
      self.cache = self._shared_cache(
          host=host,
          port=port,
          db=db,
          uri=uri,
      )
    else:
      self.cache = self._create_cache(
          host=host,
          port=port,
          db=db,
          uri=uri,
      )

  @classmethod
  def _shared_cache(
      cls,
      *,
      host: str,
      port: int,
      db: int,
      uri: Optional[str],
  ) -> redis.Redis:
    cache_key = cls._cache_key(
        host=host,
        port=port,
        db=db,
        uri=uri,
    )
    with cls._shared_caches_lock:
      cache = cls._shared_caches.get(cache_key)
      if cache is None:
        cache = cls._create_cache(
            host=host,
            port=port,
            db=db,
            uri=uri,
        )
        cls._shared_caches[cache_key] = cache
      return cache

  @staticmethod
  def _create_cache(
      *,
      host: str,
      port: int,
      db: int,
      uri: Optional[str],
  ) -> redis.Redis:
    return (
        redis.Redis.from_url(uri)
        if uri
        else redis.Redis(host=host, port=port, db=db)
    )

  @staticmethod
  def _cache_key(
      *,
      host: str,
      port: int,
      db: int,
      uri: Optional[str],
  ) -> tuple[str, ...]:
    if uri:
      return ("uri", uri)
    return ("tcp", str(host), str(port), str(db))

  @override
  async def create_session(
      self,
      *,
      app_name: str,
      user_id: str,
      state: Optional[dict[str, Any]] = None,
      session_id: Optional[str] = None,
  ) -> Session:
    return await self._create_session_impl(
        app_name=app_name,
        user_id=user_id,
        state=state,
        session_id=session_id,
    )

  def create_session_sync(
      self,
      *,
      app_name: str,
      user_id: str,
      state: Optional[dict[str, Any]] = None,
      session_id: Optional[str] = None,
  ) -> Session:
    logger.warning("Deprecated. Please migrate to the async method.")
    import asyncio

    return asyncio.run(
        self._create_session_impl(
            app_name=app_name,
            user_id=user_id,
            state=state,
            session_id=session_id,
        )
    )

  async def _create_session_impl(
      self,
      *,
      app_name: str,
      user_id: str,
      state: Optional[dict[str, Any]] = None,
      session_id: Optional[str] = None,
  ) -> Session:
    session_id = self._normalize_session_id(session_id)

    state_deltas = _session_util.extract_state_delta(state or {})
    app_state_delta = state_deltas["app"]
    user_state_delta = state_deltas["user"]
    session_state = state_deltas["session"]
    session = Session(
        app_name=app_name,
        user_id=user_id,
        id=session_id,
        state=session_state or {},
        last_update_time=time.time(),
    )
    session_key = self._session_key(app_name, user_id, session_id)

    async with self._session_write_lock(session_key):
      created = await self._create_session_in_storage(
          session_key=session_key,
          app_name=app_name,
          user_id=user_id,
          session=session,
          app_state_delta=app_state_delta,
          user_state_delta=user_state_delta,
      )
    if not created:
      raise AlreadyExistsError(f"Session with id {session_id} already exists.")

    copied_session = copy.deepcopy(session)
    return await self._merge_state(app_name, user_id, copied_session)

  async def get_or_create_session(
      self,
      *,
      app_name: str,
      user_id: str,
      state: Optional[dict[str, Any]] = None,
      session_id: Optional[str] = None,
  ) -> Session:
    return await self._get_or_create_session_impl(
        app_name=app_name,
        user_id=user_id,
        state=state,
        session_id=session_id,
    )

  def get_or_create_session_sync(
      self,
      *,
      app_name: str,
      user_id: str,
      state: Optional[dict[str, Any]] = None,
      session_id: Optional[str] = None,
  ) -> Session:
    logger.warning("Deprecated. Please migrate to the async method.")
    import asyncio

    return asyncio.run(
        self._get_or_create_session_impl(
            app_name=app_name,
            user_id=user_id,
            state=state,
            session_id=session_id,
        )
    )

  async def _get_or_create_session_impl(
      self,
      *,
      app_name: str,
      user_id: str,
      state: Optional[dict[str, Any]] = None,
      session_id: Optional[str] = None,
  ) -> Session:
    normalized_session_id = self._normalize_session_id(session_id)

    loaded = await self._get_session_impl(
        app_name=app_name,
        user_id=user_id,
        session_id=normalized_session_id,
    )
    if loaded is not None:
      return loaded

    try:
      return await self._create_session_impl(
          app_name=app_name,
          user_id=user_id,
          state=state,
          session_id=normalized_session_id,
      )
    except AlreadyExistsError:
      loaded = await self._get_session_impl(
          app_name=app_name,
          user_id=user_id,
          session_id=normalized_session_id,
      )
      if loaded is not None:
        return loaded
      raise

  @override
  async def get_session(
      self,
      *,
      app_name: str,
      user_id: str,
      session_id: str,
      config: Optional[GetSessionConfig] = None,
  ) -> Optional[Session]:
    return await self._get_session_impl(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        config=config,
    )

  def get_session_sync(
      self,
      *,
      app_name: str,
      user_id: str,
      session_id: str,
      config: Optional[GetSessionConfig] = None,
  ) -> Optional[Session]:
    logger.warning("Deprecated. Please migrate to the async method.")
    import asyncio

    return asyncio.run(
        self._get_session_impl(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            config=config,
        )
    )

  async def _get_session_impl(
      self,
      *,
      app_name: str,
      user_id: str,
      session_id: str,
      config: Optional[GetSessionConfig] = None,
  ) -> Optional[Session]:
    session = await self._load_session(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )
    if session is None:
      return None

    copied_session = copy.deepcopy(session)

    if config:
      if config.num_recent_events:
        copied_session.events = copied_session.events[
            -config.num_recent_events :
        ]
      if config.after_timestamp:
        i = len(copied_session.events) - 1
        while i >= 0:
          if copied_session.events[i].timestamp < config.after_timestamp:
            break
          i -= 1
        if i >= 0:
          copied_session.events = copied_session.events[i + 1 :]

    return await self._merge_state(app_name, user_id, copied_session)

  @override
  async def list_sessions(
      self, *, app_name: str, user_id: Optional[str] = None
  ) -> ListSessionsResponse:
    return await self._list_sessions_impl(app_name=app_name, user_id=user_id)

  def list_sessions_sync(
      self, *, app_name: str, user_id: Optional[str] = None
  ) -> ListSessionsResponse:
    logger.warning("Deprecated. Please migrate to the async method.")
    import asyncio

    return asyncio.run(
        self._list_sessions_impl(app_name=app_name, user_id=user_id)
    )

  async def _list_sessions_impl(
      self, *, app_name: str, user_id: Optional[str] = None
  ) -> ListSessionsResponse:
    sessions_without_events = []
    session_keys = await self._list_session_keys(app_name, user_id)
    for session_key in session_keys:
      session = await self._load_session_by_key(session_key)
      if session is None:
        continue
      copied_session = copy.deepcopy(session)
      copied_session.events = []
      copied_session = await self._merge_state(
          app_name, copied_session.user_id, copied_session
      )
      sessions_without_events.append(copied_session)

    return ListSessionsResponse(sessions=sessions_without_events)

  @override
  async def delete_session(
      self, *, app_name: str, user_id: str, session_id: str
  ) -> None:
    await self._delete_session_impl(
        app_name=app_name, user_id=user_id, session_id=session_id
    )

  def delete_session_sync(
      self, *, app_name: str, user_id: str, session_id: str
  ) -> None:
    logger.warning("Deprecated. Please migrate to the async method.")
    import asyncio

    asyncio.run(
        self._delete_session_impl(
            app_name=app_name, user_id=user_id, session_id=session_id
        )
    )

  async def _delete_session_impl(
      self, *, app_name: str, user_id: str, session_id: str
  ) -> None:
    session_key = self._session_key(app_name, user_id, session_id)
    async with self._session_write_lock(session_key):
      await self.cache.delete(session_key)

  @override
  async def append_event(self, session: Session, event: Event) -> Event:
    if event.partial:
      return event

    session_key = self._session_key(
        session.app_name,
        session.user_id,
        session.id,
    )
    async with self._session_write_lock(session_key):

      def _warning(message: str) -> None:
        logger.warning(
            "Failed to append event to session %s: %s", session.id, message
        )

      appended_to_session = False
      app_state_delta: dict[str, Any] = {}
      user_state_delta: dict[str, Any] = {}
      session_state_delta: dict[str, Any] = {}

      for attempt in range(self.max_transaction_retries):
        try:
          async with self.cache.pipeline(transaction=True) as pipe:
            await pipe.watch(session_key)
            raw = await pipe.get(session_key)
            if not raw:
              _warning("session_id not in sessions storage")
              return event

            if not appended_to_session:
              await super().append_event(session=session, event=event)
              session.last_update_time = event.timestamp
              if event.actions and event.actions.state_delta:
                state_deltas = _session_util.extract_state_delta(
                    event.actions.state_delta
                )
                app_state_delta = state_deltas["app"]
                user_state_delta = state_deltas["user"]
                session_state_delta = state_deltas["session"]
              appended_to_session = True

            storage_session = _session_from_dict(self._decode_session(raw))
            storage_session.events.append(event)
            storage_session.last_update_time = event.timestamp
            if session_state_delta:
              storage_session.state.update(session_state_delta)

            pipe.multi()
            if app_state_delta:
              self._queue_hash_state_updates(
                  pipe,
                  key=f"{State.APP_PREFIX}{session.app_name}",
                  state_delta=app_state_delta,
              )
            if user_state_delta:
              self._queue_hash_state_updates(
                  pipe,
                  key=(
                      f"{State.USER_PREFIX}{session.app_name}:"
                      f"{session.user_id}"
                  ),
                  state_delta=user_state_delta,
              )
            pipe.set(
                session_key,
                json.dumps(
                    _session_to_dict(storage_session),
                    default=_json_serializer,
                ),
            )
            pipe.expire(session_key, self.expire)
            await pipe.execute()
            return event
        except WatchError:
          delay = self._transaction_retry_delay(attempt)
          if delay > 0:
            await asyncio.sleep(delay)
          continue

      raise RuntimeError(
          "Failed to append event after retrying concurrent writes. "
          f"app_name={session.app_name}, user_id={session.user_id}, "
          f"session_id={session.id}, retries={self.max_transaction_retries}"
      )

  async def _merge_state(
      self, app_name: str, user_id: str, session: Session
  ) -> Session:
    app_state = await self.cache.hgetall(f"{State.APP_PREFIX}{app_name}")
    for k, v in app_state.items():
      session.state[State.APP_PREFIX + k.decode()] = json.loads(v.decode())

    user_state_key = f"{State.USER_PREFIX}{app_name}:{user_id}"
    user_state = await self.cache.hgetall(user_state_key)
    for k, v in user_state.items():
      session.state[State.USER_PREFIX + k.decode()] = json.loads(v.decode())

    return session

  async def _load_session(
      self, *, app_name: str, user_id: str, session_id: str
  ) -> Optional[Session]:
    return await self._load_session_by_key(
        self._session_key(app_name, user_id, session_id)
    )

  async def _load_session_by_key(self, key: str) -> Optional[Session]:
    raw = await self.cache.get(key)
    if not raw:
      return None
    return _session_from_dict(self._decode_session(raw))

  async def _create_session_in_storage(
      self,
      *,
      session_key: str,
      app_name: str,
      user_id: str,
      session: Session,
      app_state_delta: dict[str, Any],
      user_state_delta: dict[str, Any],
  ) -> bool:
    session_payload = json.dumps(
        _session_to_dict(session),
        default=_json_serializer,
    )

    for attempt in range(self.max_transaction_retries):
      try:
        async with self.cache.pipeline(transaction=True) as pipe:
          await pipe.watch(session_key)
          raw = await pipe.get(session_key)
          if raw is not None:
            return False

          pipe.multi()
          if app_state_delta:
            self._queue_hash_state_updates(
                pipe,
                key=f"{State.APP_PREFIX}{app_name}",
                state_delta=app_state_delta,
            )
          if user_state_delta:
            self._queue_hash_state_updates(
                pipe,
                key=f"{State.USER_PREFIX}{app_name}:{user_id}",
                state_delta=user_state_delta,
            )
          pipe.set(session_key, session_payload)
          pipe.expire(session_key, self.expire)
          await pipe.execute()
          return True
      except WatchError:
        delay = self._transaction_retry_delay(attempt)
        if delay > 0:
          await asyncio.sleep(delay)
        continue

    if await self._load_session_by_key(session_key) is not None:
      return False

    raise RuntimeError(
        "Failed to create session after retrying concurrent writes. "
        f"app_name={app_name}, user_id={user_id}, session_id={session.id}, "
        f"retries={self.max_transaction_retries}"
    )

  def _queue_hash_state_updates(
      self, pipe: Any, *, key: str, state_delta: dict[str, Any]
  ) -> None:
    for state_key, value in state_delta.items():
      pipe.hset(
          key,
          state_key,
          json.dumps(value, default=_json_serializer),
      )

  def _normalize_session_id(self, session_id: Optional[str]) -> str:
    return (
        session_id.strip()
        if session_id and session_id.strip()
        else str(uuid.uuid4())
    )

  def _session_write_lock(self, storage_key: str) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock_key = (id(self.cache), storage_key)
    with self._session_write_locks_lock:
      loop_locks = self._session_write_locks.setdefault(loop, {})
      lock = loop_locks.get(lock_key)
      if lock is None:
        lock = asyncio.Lock()
        loop_locks[lock_key] = lock
      return lock

  def _transaction_retry_delay(self, attempt: int) -> float:
    if self.transaction_retry_base_delay <= 0:
      return 0.0

    delay = min(
        self.transaction_retry_base_delay * (2**attempt),
        self.transaction_retry_max_delay,
    )
    return delay + random.uniform(0.0, self.transaction_retry_base_delay)

  def _decode_session(self, raw: bytes | None) -> dict[str, Any]:
    if not raw:
      return {}
    return json.loads(raw.decode())

  def _session_key(self, app_name: str, user_id: str, session_id: str) -> str:
    return f"{SESSION_PREFIX}{app_name}:{user_id}:{session_id}"

  def _session_pattern(
      self, app_name: str, user_id: Optional[str] = None
  ) -> str:
    if user_id is None:
      return f"{SESSION_PREFIX}{app_name}:*"
    return f"{SESSION_PREFIX}{app_name}:{user_id}:*"

  async def _list_session_keys(
      self, app_name: str, user_id: Optional[str] = None
  ) -> list[str]:
    pattern = self._session_pattern(app_name, user_id)
    keys: list[str] = []
    if hasattr(self.cache, "scan_iter"):
      async for key in self.cache.scan_iter(match=pattern):
        keys.append(key.decode() if isinstance(key, bytes) else key)
      return keys
    if hasattr(self.cache, "keys"):
      raw_keys = await self.cache.keys(pattern)
      for key in raw_keys:
        keys.append(key.decode() if isinstance(key, bytes) else key)
    return keys
