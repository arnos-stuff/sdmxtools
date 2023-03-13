import requests
import sdmx
import logging
import asyncio as aio
import pandas as pd
import numpy as np
import sys
import os
import time
import json
import rich
import aiohttp

from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable
from pathlib import Path
from ..utils import dlProgress as progress, console

sys.setrecursionlimit(50000)

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

async def remote_toc_build(
    agency,
    remote_server_url,
    dataflow_ids: Iterable[int],
    dest_dir: "tmp",
    chunk_size: int = 100,
    ):
    # sourcery skip: use-contextlib-suppress
    """Download all Dataflow tables of contents from the SDMX API."""
    
    async def download_dataflow(dataflow_id):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                remote_server_url,
                headers={"Content-Type":"application/json"},
                data=json.dumps({
                    "dataflow_id": dataflow_id,
                    "flow_id": dataflow_id,
                    "agency": agency
                    })
                ) as resp:
                assert resp.status == 200
                
    loop=aio.new_event_loop()
    with ThreadPoolExecutor(max_workers=200) as executor:
        for i, fid in enumerate(dataflow_ids):
            loop.run_in_executor(executor, aio.ensure_future(download_dataflow(fid)))
            rich.print(f"[bold][yellow]Added[/yellow] [red]{fid}[/red] to [cyan]background tasks[/cyan][/bold]")
        
    rich.print(f"[bold][yellow]Done sending[/yellow] [cyan]{i}[/cyan] [yellow]tasks to complete[/yellow][/bold]")
    


def run():  # sourcery skip: extract-method
    if len(sys.argv) < 3:
        console.print("Usage: python remote_build_toc.py <agency> <remote>", style="red")
        sys.exit(1)
    if len(sys.argv) > 3:
        maxnb = int(sys.argv[3]) if sys.argv[3].isdigit() else None
    else:
        maxnb = None
    url = sys.argv[2]
    agency = sys.argv[1]
    agency = agency.upper()

    if agency not in sdmx.list_sources():
        console.print(f"Agency {agency} not found in SDMX API", style="red")
        sys.exit(1)

    dest_dir = Path("tmp")
    dest_dir.mkdir(parents=True, exist_ok=True)
    client = sdmx.Client(agency)

    flows_path = dest_dir / agency / "flows.csv"

    if flows_path.exists():
        df = pd.read_csv(flows_path)
        flow_ids = df.dataflow_id.unique()
    else:
        with progress:
            init_task = progress.add_task(filename=f"Getting flows for {agency}", description="Downloading all flows..", total=None)
            allflow = client.dataflow()
            dfallflow = sdmx.to_pandas(allflow)
            df = pd.DataFrame(dfallflow["dataflow"])
            df = df.reset_index()
            df.columns = ["dataflow_id", "dataflow_description"]
            progress.remove_task(task_id=init_task)
            progress.console.log(f"Found {len(df)} dataflows for {agency}", style="green")
        dest_dir =  dest_dir / agency
        dest_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(dest_dir / "flows.csv", index=False)
        flow_ids = df.dataflow_id.unique()
        
    console.print(f"Found {len(flow_ids)} dataflows for {agency}", style="green")
    console.print(f"Launching {len(flow_ids)} requests to [yellow]{url}[/yellow]", style="green")
    if maxnb:
        flow_ids = flow_ids[:maxnb]
    aio.run(remote_toc_build(agency, url, flow_ids, dest_dir))
    
