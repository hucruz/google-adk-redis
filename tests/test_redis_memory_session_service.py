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
async def test_create_session_applies_initial_state_delta(
    session_service: RedisMemorySessionService,
):
  session = await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
      state={
          f"{State.APP_PREFIX}theme": "dark",
          f"{State.USER_PREFIX}locale": "es",
          f"{State.TEMP_PREFIX}scratch": "ignore",
          "counter": 3,
      },
  )

  loaded = await session_service.get_session(
      app_name="demo-app",
      user_id="user-123",
      session_id=session.id,
  )

  assert loaded is not None
  assert loaded.state[f"{State.APP_PREFIX}theme"] == "dark"
  assert loaded.state[f"{State.USER_PREFIX}locale"] == "es"
  assert loaded.state["counter"] == 3
  assert f"{State.TEMP_PREFIX}scratch" not in loaded.state


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
async def test_append_event_skips_partial_events(
    session_service: RedisMemorySessionService,
):
  session = await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
  )

  event = Event(
      author="user",
      partial=True,
      actions=EventActions(
          state_delta={
              f"{State.APP_PREFIX}theme": "light",
              "counter": 9,
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
  assert loaded.events == []
  assert f"{State.APP_PREFIX}theme" not in loaded.state
  assert "counter" not in loaded.state


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
async def test_list_sessions_clears_events_and_merges_state(
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
  assert listed.state[f"{State.APP_PREFIX}color"] == "blue"


@pytest.mark.asyncio
async def test_list_sessions_for_all_users_merges_state(
    session_service: RedisMemorySessionService,
):
  session_one = await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
      state={
          f"{State.APP_PREFIX}theme": "dark",
          f"{State.USER_PREFIX}locale": "es",
          "counter": 1,
      },
  )
  session_two = await session_service.create_session(
      app_name="demo-app",
      user_id="user-456",
      state={"counter": 2},
  )

  response = await session_service.list_sessions(
      app_name="demo-app",
      user_id=None,
  )

  assert len(response.sessions) == 2
  sessions_by_user = {session.user_id: session for session in response.sessions}
  assert sessions_by_user["user-123"].id == session_one.id
  assert sessions_by_user["user-456"].id == session_two.id
  for session in response.sessions:
    assert session.events == []
    assert session.state[f"{State.APP_PREFIX}theme"] == "dark"
  assert (
      sessions_by_user["user-123"].state[f"{State.USER_PREFIX}locale"] == "es"
  )
  assert (
      f"{State.USER_PREFIX}locale" not in sessions_by_user["user-456"].state
  )
  assert sessions_by_user["user-123"].state["counter"] == 1
  assert sessions_by_user["user-456"].state["counter"] == 2


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
