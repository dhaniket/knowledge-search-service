from datetime import UTC, datetime

CHANNELS = ("rabbitmq", "kafka")


def new_outbox_state() -> dict:
    now = datetime.now(UTC)

    return {
        channel: {
            "status": "pending",
            "attempts": 0,
            "next_attempt_at": now,
        }
        for channel in CHANNELS
    }
