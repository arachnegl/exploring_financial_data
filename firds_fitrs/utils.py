import concurrent.futures
import multiprocessing
from collections.abc import Callable
from typing import Any


def run_concurrent(
    cb: Callable,
    id_cb_args_cb_kwargs: list[tuple[Any, tuple, dict]],
    **kwargs,
):
    results = {}
    num_cpus = multiprocessing.cpu_count()
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=num_cpus, **kwargs
    ) as executor:
        future_to_id = {
            executor.submit(cb, *cb_args, **cb_kwargs): id
            for id, cb_args, cb_kwargs in id_cb_args_cb_kwargs
        }
        for future in concurrent.futures.as_completed(future_to_id):
            id = future_to_id[future]
            try:
                result = future.result()
            except Exception as exc:
                print(f"{id} failed {exc}")
                continue
            results[id] = result

    return results
