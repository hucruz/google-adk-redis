from __future__ import annotations

import pytest

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.base_session_service import GetSessionConfig
from google.adk.sessions.state import State

from google_adk_redis.redis_memory_session_service import (
  RedisMemorySessionService,
)


@pytest.mark.asyncio
async def test_create_and_get_session(session_service: RedisMemorySessionService):
  session = await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
  )

  loaded = await session_service.get_session(
      app_name="demo-app",
      user_id="user-123",
      session_id=session.id,
  )

  assert loaded is not None
  assert loaded.id == session.id
  assert loaded.events == []
  assert loaded.state == {}


@pytest.mark.asyncio
async def test_append_event_persists_state_and_events(
    session_service: RedisMemorySessionService,
):
  session = await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
  )

  event = Event(
      author="user",
      actions=EventActions(
          state_delta={
              f"{State.APP_PREFIX}theme": "dark",
              f"{State.USER_PREFIX}locale": "es",
              f"{State.TEMP_PREFIX}scratch": "ignore",
              "counter": 1,
          }
      ),
  )

  await session_service.append_event(session, event)

  loaded = await session_service.get_session(
      app_name="demo-app",
      user_id="user-123",
      session_id=session.id,
  )

  assert loaded is not None
  assert len(loaded.events) == 1
  assert loaded.events[0].id == event.id
  assert loaded.state[f"{State.APP_PREFIX}theme"] == "dark"
  assert loaded.state[f"{State.USER_PREFIX}locale"] == "es"
  assert loaded.state["counter"] == 1
  assert f"{State.TEMP_PREFIX}scratch" not in loaded.state


@pytest.mark.asyncio
async def test_get_session_filters_recent_events(
    session_service: RedisMemorySessionService,
):
  session = await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
  )

  first = Event(author="user")
  await session_service.append_event(session, first)
  second = Event(author="user")
  await session_service.append_event(session, second)

  loaded = await session_service.get_session(
      app_name="demo-app",
      user_id="user-123",
      session_id=session.id,
      config=GetSessionConfig(num_recent_events=1),
  )

  assert loaded is not None
  assert len(loaded.events) == 1
  assert loaded.events[0].id == second.id


@pytest.mark.asyncio
async def test_list_sessions_clears_state_and_events(
    session_service: RedisMemorySessionService,
):
  session = await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
  )
  event = Event(
      author="user",
      actions=EventActions(
          state_delta={f"{State.APP_PREFIX}color": "blue"}
      ),
  )
  await session_service.append_event(session, event)

  response = await session_service.list_sessions(
      app_name="demo-app",
      user_id="user-123",
  )

  assert len(response.sessions) == 1
  listed = response.sessions[0]
  assert listed.id == session.id
  assert listed.events == []
  assert listed.state == {}


@pytest.mark.asyncio
async def test_delete_session_removes_data(
    session_service: RedisMemorySessionService,
):
  session = await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
  )

  await session_service.delete_session(
      app_name="demo-app",
      user_id="user-123",
      session_id=session.id,
  )

  loaded = await session_service.get_session(
      app_name="demo-app",
      user_id="user-123",
      session_id=session.id,
  )

  assert loaded is None
