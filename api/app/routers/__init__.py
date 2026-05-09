from datetime import datetime, timezone
from typing import Any

from ..schemas import Envelope, Meta


def envelope(data: list[Any]) -> dict[str, Any]:
    return {
        "data": data,
        "meta": {
            "count": len(data),
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        },
    }
