import asyncio
from time import perf_counter
import time


async def blocking_operation(
    name: str,
) -> None:

    print(f"{name} started")

    time.sleep(2)

    print(f"{name} completed")


def legacy_blocking_call(
    name: str,
) -> str:

    print(f"{name} started")

    time.sleep(2)

    print(f"{name} completed")

    return f"{name} result"


async def demonstrate_to_thread() -> None:

    print("\n--- TO THREAD ---")

    start = perf_counter()

    results = await asyncio.gather(
        asyncio.to_thread(
            legacy_blocking_call,
            "Legacy 1",
        ),
        asyncio.to_thread(
            legacy_blocking_call,
            "Legacy 2",
        ),
        asyncio.to_thread(
            legacy_blocking_call,
            "Legacy 3",
        ),
    )

    elapsed = perf_counter() - start

    print(results)

    print(f"to_thread time: " f"{elapsed:.2f}s")


async def demonstrate_blocking() -> None:

    print("\n--- BLOCKING INSIDE ASYNC ---")

    start = perf_counter()

    await asyncio.gather(
        blocking_operation("Operation 1"),
        blocking_operation("Operation 2"),
        blocking_operation("Operation 3"),
    )

    elapsed = perf_counter() - start

    print(f"Blocking time: " f"{elapsed:.2f}s")


async def simulated_api_call(
    name: str,
    delay: float,
) -> str:

    print(f"{name} started")

    await asyncio.sleep(delay)

    print(f"{name} completed")

    return f"{name} result"


async def run_sequentially() -> None:

    print("\n--- SEQUENTIAL ---")

    start = perf_counter()

    result_1 = await simulated_api_call(
        "MongoDB",
        2,
    )

    result_2 = await simulated_api_call(
        "Elasticsearch",
        2,
    )

    result_3 = await simulated_api_call(
        "Redis",
        2,
    )

    elapsed = perf_counter() - start

    print(
        result_1,
        result_2,
        result_3,
    )

    print(f"Sequential time: " f"{elapsed:.2f}s")


async def run_concurrently() -> None:

    print("\n--- CONCURRENT ---")

    start = perf_counter()

    results = await asyncio.gather(
        simulated_api_call(
            "MongoDB",
            2,
        ),
        simulated_api_call(
            "Elasticsearch",
            2,
        ),
        simulated_api_call(
            "Redis",
            2,
        ),
    )

    elapsed = perf_counter() - start

    print(results)

    print(f"Concurrent time: " f"{elapsed:.2f}s")


async def demonstrate_task_group() -> None:

    print("\n--- TASK GROUP ---")

    start = perf_counter()

    async with asyncio.TaskGroup() as group:

        mongo_task = group.create_task(
            simulated_api_call(
                "MongoDB TG",
                2,
            )
        )

        elastic_task = group.create_task(
            simulated_api_call(
                "Elasticsearch TG",
                2,
            )
        )

        redis_task = group.create_task(
            simulated_api_call(
                "Redis TG",
                2,
            )
        )

    results = [
        mongo_task.result(),
        elastic_task.result(),
        redis_task.result(),
    ]

    elapsed = perf_counter() - start

    print(results)

    print(f"TaskGroup time: " f"{elapsed:.2f}s")


async def main() -> None:

    await run_sequentially()

    await run_concurrently()

    await demonstrate_blocking()

    await demonstrate_to_thread()

    await demonstrate_task_group()


if __name__ == "__main__":

    asyncio.run(main())
