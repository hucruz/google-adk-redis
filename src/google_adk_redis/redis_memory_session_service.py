# Copyright 2025 The project authors
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
# Credits:
# - BloodBoy21, https://github.com/BloodBoy21/nerds-adk-python/tree/feat-redis-session
from __future__ import annotations

import base64
import copy
import datetime
from decimal import Decimal
import json
import logging
import math
import time
from typing import Any
from typing import Optional
import uuid

import redis.asyncio as redis
from typing_extensions import override

from google.adk.events.event import Event
from google.adk.sessions.base_session_service import BaseSessionService
from google.adk.sessions.base_session_service import GetSessionConfig
from google.adk.sessions.base_session_service import ListSessionsResponse
from google.adk.sessions.session import Session
from google.adk.sessions.state import State

logger = logging.getLogger("google_adk_redis." + __name__)

DEFAULT_EXPIRATION = 60 * 60  # 1 hour


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


class RedisMemorySessionService(BaseSessionService):
  """A Redis-backed implementation of the session service."""

  def __init__(
      self,
      host="localhost",
      port=6379,
      db=0,
      uri=None,
      expire=DEFAULT_EXPIRATION,
  ):
    self.host = host
    self.port = port
    self.db = db
    self.uri = uri
    self.expire = expire

    self.cache = (
        redis.Redis.from_url(uri)
        if uri
        else redis.Redis(host=host, port=port, db=db)
    )

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
    session_id = (
        session_id.strip()
        if session_id and session_id.strip()
        else str(uuid.uuid4())
    )
    session = Session(
        app_name=app_name,
        user_id=user_id,
        id=session_id,
        state=state or {},
        last_update_time=time.time(),
    )

    sessions = await self._load_sessions(app_name, user_id)
    sessions[session_id] = session.to_dict()
    await self._save_sessions(app_name, user_id, sessions)

    copied_session = copy.deepcopy(session)
    return await self._merge_state(app_name, user_id, copied_session)

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
    sessions = await self._load_sessions(app_name, user_id)
    if session_id not in sessions:
      return None

    session = Session.from_dict(sessions[session_id])
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
      self, *, app_name: str, user_id: str
  ) -> ListSessionsResponse:
    return await self._list_sessions_impl(app_name=app_name, user_id=user_id)

  def list_sessions_sync(
      self, *, app_name: str, user_id: str
  ) -> ListSessionsResponse:
    logger.warning("Deprecated. Please migrate to the async method.")
    import asyncio

    return asyncio.run(
        self._list_sessions_impl(app_name=app_name, user_id=user_id)
    )

  async def _list_sessions_impl(
      self, *, app_name: str, user_id: str
  ) -> ListSessionsResponse:
    sessions = await self._load_sessions(app_name, user_id)
    sessions_without_events = []

    for session_data in sessions.values():
      session = Session.from_dict(session_data)
      copied_session = copy.deepcopy(session)
      copied_session.events = []
      copied_session.state = {}
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
    if (
        await self._get_session_impl(
            app_name=app_name, user_id=user_id, session_id=session_id
        )
        is None
    ):
      return

    sessions = await self._load_sessions(app_name, user_id)
    if session_id in sessions:
      del sessions[session_id]
      await self._save_sessions(app_name, user_id, sessions)

  @override
  async def append_event(self, session: Session, event: Event) -> Event:
    await super().append_event(session=session, event=event)
    session.last_update_time = event.timestamp

    if event.actions and event.actions.state_delta:
      for key, value in event.actions.state_delta.items():
        if key.startswith(State.APP_PREFIX):
          await self.cache.hset(
              f"{State.APP_PREFIX}{session.app_name}",
              key.removeprefix(State.APP_PREFIX),
              json.dumps(value),
          )
        if key.startswith(State.USER_PREFIX):
          await self.cache.hset(
              f"{State.USER_PREFIX}{session.app_name}:{session.user_id}",
              key.removeprefix(State.USER_PREFIX),
              json.dumps(value),
          )

    sessions = await self._load_sessions(session.app_name, session.user_id)
    sessions[session.id] = session.to_dict()
    await self._save_sessions(session.app_name, session.user_id, sessions)

    return event

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

  async def _load_sessions(
      self, app_name: str, user_id: str
  ) -> dict[str, dict]:
    key = f"{State.APP_PREFIX}{app_name}:{user_id}"
    raw = await self.cache.get(key)
    if not raw:
      return {}
    raw_data = json.loads(raw.decode())
    return raw_data

  async def _save_sessions(
      self, app_name: str, user_id: str, sessions: dict[str, Any]
  ):
    key = f"{State.APP_PREFIX}{app_name}:{user_id}"
    await self.cache.set(key, json.dumps(sessions, default=_json_serializer))
    await self.cache.expire(key, self.expire)
