# google-adk-redis

Redis-backed session service for Google ADK.

## Install

From a Git repo:

```bash
pip install git+https://github.com/hucruz/google-adk-redis.git
```

From PyPI:

```bash
pip install google-adk-redis
```

## Usage (programmatic)

```python
from google_adk_redis import RedisMemorySessionService

session_service = RedisMemorySessionService(
    host="localhost",
    port=6379,
    db=0,
    expire=60 * 60,
    max_transaction_retries=16,
)
```

Instances created with the same Redis configuration reuse the same underlying
async Redis client by default, so creating many services does not multiply
connection pools. You can opt out with `share_cache=False` or inject a custom
client with `cache=...`.

If your application may attempt to load and create the same known session ID
concurrently, prefer `get_or_create_session(...)` over a manual
`get_session(...)` then `create_session(...)` sequence.

Writers for the same `app_name` and `user_id` are serialized within a single
Python process to avoid clobbering the shared session blob. If you still expect
cross-process contention, increase `max_transaction_retries` and, if needed,
adjust `transaction_retry_base_delay` / `transaction_retry_max_delay`.

### Optional register for `google.adk.sessions`

```python
import google_adk_redis

google_adk_redis.register()
from google.adk.sessions import RedisMemorySessionService
```

## Usage with Google ADK (Runner)

```python
import asyncio

from google.adk.agents.llm_agent import LlmAgent
from google.adk.runners import Runner
from google.genai import types

from google_adk_redis import RedisMemorySessionService


async def main():
    session_service = RedisMemorySessionService(
        host="localhost",
        port=6379,
        db=0,
        expire=60 * 60,
    )
    agent = LlmAgent(
        name="assistant",
        model="gemini-2.0-flash",
        instruction="You are a helpful assistant.",
    )

    async with Runner(
        app_name="demo-app",
        agent=agent,
        session_service=session_service,
    ) as runner:
        session = await session_service.create_session(
            app_name="demo-app",
            user_id="user-123",
        )

        async for event in runner.run_async(
            user_id=session.user_id,
            session_id=session.id,
            new_message=types.Content(
                role="user",
                parts=[types.Part(text="Hola!")],
            ),
        ):
            if event.content and event.content.parts:
                text = "".join(part.text or "" for part in event.content.parts)
                if text:
                    print(text)


asyncio.run(main())
```

## Authors

- BloodBoy21 (original implementation, https://github.com/BloodBoy21)
- hucruz (upstream contributions, https://github.com/hucruz)

## Maintainer

- hucruz (https://github.com/hucruz)

## Attribution

Source repository:
https://github.com/BloodBoy21/nerds-adk-python/tree/feat-redis-session

This repository packages and adapts the upstream implementation for
standalone use.
