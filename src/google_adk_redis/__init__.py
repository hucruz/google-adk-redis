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

from __future__ import annotations

from .redis_memory_session_service import RedisMemorySessionService
from .version import __version__


def register() -> None:
  """Registers RedisMemorySessionService in google.adk.sessions."""
  try:
    from google.adk import sessions as adk_sessions
  except ImportError as exc:
    raise ImportError(
        "google-adk is required to register RedisMemorySessionService."
    ) from exc

  setattr(adk_sessions, "RedisMemorySessionService", RedisMemorySessionService)
  if hasattr(adk_sessions, "__all__"):
    if "RedisMemorySessionService" not in adk_sessions.__all__:
      adk_sessions.__all__.append("RedisMemorySessionService")


__all__ = [
    "RedisMemorySessionService",
    "register",
    "__version__",
]
