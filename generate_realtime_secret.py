from __future__ import annotations

import secrets
from pathlib import Path

secret = secrets.token_hex(32)
print(secret)

out = Path("REALTIME-SECRET.txt")
out.write_text(
    "FunFernus Realtime secret\n"
    "==========================\n"
    f"{secret}\n\n"
    "Use the SAME value in:\n"
    "1) Discord bot .env -> REALTIME_SECRET\n"
    "2) Website /private/funfernus/realtime.php -> secret\n\n"
    "Do not publish this file. Delete it after configuration.\n",
    encoding="utf-8",
)
print(f"Saved to {out.resolve()}")
