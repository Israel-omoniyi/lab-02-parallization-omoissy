"""
Module to get brewery information via the https://www.openbrewerydb.org/ API.
"""

import time
import asyncio
import requests as req
from pathlib import Path
from typing import List, Dict
import json
import socket


def get_brewery_count(state: str) -> Dict:
    """
    Retrieves information about breweries in a state and counts the number
    of breweries. Use page limit of 200 which is the max supported by the
    Open Brewery DB API https://www.openbrewerydb.org/documentation.
    We use a combination of page and per_page arguments to get information
    about all the breweries in a state since a single API call only returns
    a maximum of 200 breweries at a time. The page parameter is incremented by
    1 in every call until we get a non 200 response or an empty response.

    Args:
        state (string): The full name of a state, case insensitive.

    Returns:
        Dict: Dictionary with state name and brewery count.
              For example: {'state': 'maryland', 'brewery_count': 109}.
    """
    print(f"get_brewery_count, state={state}, entry")
    count = 0

    base_url = "https://api.openbrewerydb.org/v1/breweries"
    per_page = 200
    page = 1

    while True:
        url = f"{base_url}?by_state={state}&per_page={per_page}&page={page}"

        try:
            resp = req.get(url, timeout=30)
        except Exception as e:
            # network error: stop and return what we have
            print(f"get_brewery_count, state={state}, request error: {e}")
            break

        if resp.status_code != 200:
            # stop if API stops responding successfully
            print(f"get_brewery_count, state={state}, status_code={resp.status_code}, stopping")
            break

        try:
            data = resp.json()
        except Exception as e:
            print(f"get_brewery_count, state={state}, json decode error: {e}")
            break

        # empty list means no more pages
        if not data:
            break

        count += len(data)
        page += 1

    print(f"get_brewery_count, state={state}, exiting")
    return dict(state=state, brewery_count=count)


async def async_get_brewery_count(state: str) -> Dict:
    """
    Wraps the get_brewery_count call into async.

    Args:
        state (string): The full name of a state, case insensitive.

    Returns:
        Dict: Dictionary with state name and brewery count
    """
    loop = asyncio.get_running_loop()
    # Run the blocking requests-based function in a background thread
    return await loop.run_in_executor(None, get_brewery_count, state)


async def get_brewery_counts_for_states(states: List[str]) -> List[Dict]:
    """
    Get count of breweries for a list of states in async manner.

    Args:
        states (List[str]): List of state names.

    Returns:
         List[Dict]: List of dictionaries containing state name and brewery count
    """
    tasks = [async_get_brewery_count(state) for state in states]
    results = await asyncio.gather(*tasks)
    return list(results)


if __name__ == "__main__":
    states = ['district_of_columbia', 'maryland', 'new_york', 'virginia']

    # async version
    s = time.perf_counter()
    brewery_counts = asyncio.run(get_brewery_counts_for_states(states))
    elapsed_async = time.perf_counter() - s
    print(f"{__file__}, brewery counts (async) -> {brewery_counts}, retrieved in {elapsed_async:0.2f} seconds")

    # serial version
    s = time.perf_counter()
    brewery_counts = [get_brewery_count(s) for s in states]
    elapsed_serial = time.perf_counter() - s
    print(f"{__file__}, brewery counts (serial) -> {brewery_counts}, retrieved in {elapsed_serial:0.2f} seconds")

    # async is faster..
    faster = 100*(elapsed_serial - elapsed_async) / elapsed_serial
    result_summary = f"async version was {faster:0.2f}% faster than the serial version"
    print(f"{__file__}, {result_summary}")

    # write result to a file
    Path("async.json").write_text(json.dumps({'result': result_summary, 'host': socket.gethostname()}))
