from __future__ import annotations

import asyncio

import pytest

from google.adk.errors.already_exists_error import AlreadyExistsError
from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.base_session_service import GetSessionConfig
from google.adk.sessions.state import State

from google_adk_redis.redis_memory_session_service import (
  RedisMemorySessionService,
)


class _FakeRedisClient:
  def __init__(self, *, source: str, config: tuple[object, ...]) -> None:
    self.source = source
    self.config = config


class _FakeRedisFactory:
  def __init__(self, created_clients: list[_FakeRedisClient]) -> None:
    self._created_clients = created_clients

  def __call__(self, *, host: str, port: int, db: int) -> _FakeRedisClient:
    client = _FakeRedisClient(
        source="tcp",
        config=(host, port, db),
    )
    self._created_clients.append(client)
    return client

  def from_url(self, uri: str) -> _FakeRedisClient:
    client = _FakeRedisClient(
        source="uri",
        config=(uri,),
    )
    self._created_clients.append(client)
    return client


@pytest.mark.parametrize(
    ("service_kwargs", "expected_source", "expected_config"),
    [
        (
            {"host": "redis-a", "port": 6379, "db": 0},
            "tcp",
            ("redis-a", 6379, 0),
        ),
        (
            {"uri": "redis://user:pass@redis-b:6380/2"},
            "uri",
            ("redis://user:pass@redis-b:6380/2",),
        ),
    ],
)
def test_services_share_cache_when_configuration_matches(
    monkeypatch: pytest.MonkeyPatch,
    service_kwargs: dict[str, object],
    expected_source: str,
    expected_config: tuple[object, ...],
):
  created_clients: list[_FakeRedisClient] = []

  monkeypatch.setattr(
      "google_adk_redis.redis_memory_session_service.redis.Redis",
      _FakeRedisFactory(created_clients),
  )

  first_service = RedisMemorySessionService(**service_kwargs)
  second_service = RedisMemorySessionService(**service_kwargs)

  assert first_service.cache is second_service.cache
  assert len(created_clients) == 1
  assert created_clients[0].source == expected_source
  assert created_clients[0].config == expected_config


def test_services_do_not_share_cache_when_opting_out(
    monkeypatch: pytest.MonkeyPatch,
):
  created_clients: list[_FakeRedisClient] = []

  monkeypatch.setattr(
      "google_adk_redis.redis_memory_session_service.redis.Redis",
      _FakeRedisFactory(created_clients),
  )

  first_service = RedisMemorySessionService(
      host="redis-a",
      port=6379,
      db=0,
      share_cache=False,
  )
  second_service = RedisMemorySessionService(
      host="redis-a",
      port=6379,
      db=0,
      share_cache=False,
  )

  assert first_service.cache is not second_service.cache
  assert len(created_clients) == 2


def test_services_do_not_share_cache_when_configuration_differs(
    monkeypatch: pytest.MonkeyPatch,
):
  created_clients: list[_FakeRedisClient] = []

  monkeypatch.setattr(
      "google_adk_redis.redis_memory_session_service.redis.Redis",
      _FakeRedisFactory(created_clients),
  )

  first_service = RedisMemorySessionService(
      host="redis-a",
      port=6379,
      db=0,
  )
  second_service = RedisMemorySessionService(
      host="redis-a",
      port=6379,
      db=1,
  )

  assert first_service.cache is not second_service.cache
  assert len(created_clients) == 2


def test_constructor_uses_explicit_cache_instance():
  explicit_cache = object()

  service = RedisMemorySessionService(cache=explicit_cache)

  assert service.cache is explicit_cache


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
async def test_create_session_with_existing_id_raises_already_exists(
    session_service: RedisMemorySessionService,
):
  await session_service.create_session(
      app_name="demo-app",
      user_id="user-123",
      session_id="session-1",
  )

  with pytest.raises(AlreadyExistsError):
    await session_service.create_session(
        app_name="demo-app",
        user_id="user-123",
        session_id="session-1",
    )


@pytest.mark.asyncio
async def test_get_or_create_session_is_idempotent_under_concurrency(
    session_service: RedisMemorySessionService,
):
  created_sessions = await asyncio.gather(
      session_service.get_or_create_session(
          app_name="demo-app",
          user_id="user-123",
          session_id="shared-session",
          state={"counter": 1},
      ),
      session_service.get_or_create_session(
          app_name="demo-app",
          user_id="user-123",
          session_id="shared-session",
          state={"counter": 2},
      ),
  )

  assert created_sessions[0].id == "shared-session"
  assert created_sessions[1].id == "shared-session"

  response = await session_service.list_sessions(
      app_name="demo-app",
      user_id="user-123",
  )

  assert len(response.sessions) == 1
  assert response.sessions[0].id == "shared-session"


@pytest.mark.asyncio
async def test_create_session_preserves_concurrent_sessions_for_same_user(
    session_service: RedisMemorySessionService,
):
  created_sessions = await asyncio.gather(
      session_service.create_session(
          app_name="demo-app",
          user_id="user-123",
          session_id="session-1",
      ),
      session_service.create_session(
          app_name="demo-app",
          user_id="user-123",
          session_id="session-2",
      ),
  )

  assert {session.id for session in created_sessions} == {
      "session-1",
      "session-2",
  }

  response = await session_service.list_sessions(
      app_name="demo-app",
      user_id="user-123",
  )

  assert {session.id for session in response.sessions} == {
      "session-1",
      "session-2",
  }


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
