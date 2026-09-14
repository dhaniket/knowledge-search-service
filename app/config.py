import os

from dotenv import load_dotenv

load_dotenv()


def _get_positive_float(
    name: str,
    default: str,
) -> float:

    value = float(
        os.getenv(
            name,
            default,
        )
    )

    if value <= 0:

        raise RuntimeError(f"{name} must be " "greater than 0")

    return value


def _get_positive_int(
    name: str,
    default: str,
) -> int:

    value = int(
        os.getenv(
            name,
            default,
        )
    )

    if value <= 0:

        raise RuntimeError(f"{name} must be " "greater than 0")

    return value


def get_redis_operation_timeout() -> float:

    return _get_positive_float(
        "REDIS_OPERATION_TIMEOUT_SECONDS",
        "1.0",
    )


def get_elasticsearch_operation_timeout() -> float:

    return _get_positive_float(
        "ELASTICSEARCH_OPERATION_TIMEOUT_SECONDS",
        "5.0",
    )


def get_reindex_concurrency() -> int:

    return _get_positive_int(
        "REINDEX_CONCURRENCY",
        "10",
    )
