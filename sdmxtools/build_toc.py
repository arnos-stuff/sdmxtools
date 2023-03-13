import contextlib
import sdmx
import rich
import pandas as pd
import numpy as np
import sys
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
import signal
from functools import partial
from threading import Event, Lock
from typing import Iterable
from urllib.request import urlopen
from sortedcollections import OrderedSet

from rich.live import Live

from pathlib import Path

from .utils import dlProgress as progress, console, TaskID

sys.setrecursionlimit(50000)

done_event = Event()

def handle_sigint(signum, frame):
    done_event.set()

signal.signal(signal.SIGINT, handle_sigint)

def counter(iterable):
    """Count elements in an iterable."""
    counts = {}
    for v in iterable:
        if v in counts:
            counts[v] += 1
        else:
            counts[v] = 1
    return counts

def countTasks(queue, flowTaskMap):
    """Count the number of tasks for each flow id."""
    counts = {}
    for task in queue:
        for fid in flowTaskMap:
            if task in flowTaskMap[fid]:
                if fid in counts:
                    counts[fid] += 1
                else:
                    counts[fid] = 1
    return counts

def remove_ext(p):
    extensions = [".csv", ".json", ".parquet", ".h5", ".pkl"]
    for ext in extensions:
        p = p.replace(ext, "")
    return p

def check_done_dest_dir(dest):
    dest = Path(dest)
    return OrderedSet(map(lambda p: remove_ext(p.stem), dest.glob("*.csv.gz")))

def updateDurations(
    durationMap,
    flowTaskMap,
    progress,
    destpath,
    timeout: int = 60):
    """Update the durations of the tasks in the progress bar.
    Prunes the tasks over the timeout.
    """
    removed = set()
    for task, start in durationMap.items():
        if task in progress._tasks:
            durationMap["duration"] = time.time() - start["start"]
            if durationMap["duration"] > timeout:
                progress.remove_task(task)
                removed |= {task}
        else:
            removed |= {task}
            
    savejson = {"timeout": timeout}
    for task in removed:
        savejson["task"] = task
        savejson["dataflow"] = flowTaskMap[task]
        savejson["duration"] = durationMap[task]
        with open(destpath / "timeouts.json", "w") as f:
            json.dump(savejson, f)
        del durationMap[task]
        del flowTaskMap[task]
    return removed


def download_toc(
    client,
    dataflow_ids: Iterable[int],
    dest_dir: str,
    chunk_size: int = None,
    replace_existing: bool = False,
    timeout: int = 60,
    ):
    # sourcery skip: use-contextlib-suppress
    """Download all Dataflow tables of contents from the SDMX API."""
    if chunk_size is None:
        cols, lines = os.get_terminal_size()
        chunk_size = int(lines - 2)
    dest_dir = Path(dest_dir)
    already_done = check_done_dest_dir(dest_dir)
    nflows = len(dataflow_ids)
    ndone = len(already_done) if replace_existing else 0
    queue = OrderedSet()
    flowTaskMap = {}
    durationMap = {}
    futures = []
    state = {
        "OK": 0,
        "KO": 0,
        "events": {},
        }
    if replace_existing:
        remaining = OrderedSet(dataflow_ids.tolist())
    else:
        remaining = OrderedSet(dataflow_ids.tolist()) - already_done
    startTime = time.time()
    lock = Lock()
    with Live(
                progress,
                refresh_per_second=30,
                auto_refresh=True,
                console=console,
                redirect_stderr=True,
                screen=True,
                ) as live:
        main_task = progress.add_task("Getting TOC", filename="Getting TOC ([green] 0 OK [/green]/ [red]0 KO[/red])", total=nflows, start=True)
        with ThreadPoolExecutor(max_workers=chunk_size) as pool:
            while ndone < nflows:
                if done_event.is_set():
                    return
                if len(queue) < chunk_size:
                    for fid in remaining - OrderedSet(flowTaskMap.values()):
                        if done_event.is_set():
                            return
                        if len(queue) >= chunk_size:
                            break
                        dest_path = dest_dir / f"{fid}.csv.gz"
                        task_id = progress.add_task("Getting TOC", filename=fid, start=True, total=None)
                        queue |= {task_id}
                        flowTaskMap[task_id] = fid
                        durationMap[task_id] = {"start":time.time()}
                        state["events"][task_id] = Event()
                        futures += [pool.submit(get_toc_for_flow_id, client, task_id, fid, dest_path, state)]
                        live.refresh()

                nbrunning = len(progress._tasks) - 1
                nbdone = chunk_size - nbrunning
                ndone += nbdone
                currTime = time.time()
                if currTime - startTime > timeout/2:
                    lock.acquire()
                    removed = updateDurations(durationMap, flowTaskMap, progress, dest_dir, timeout=timeout)
                    lock.release()
                    startTime = time.time()
                else:
                    removed = set()
                for task in removed:
                    ftr = futures[queue.index(task)]
                    ftr.cancel()
                    state["events"][task].set()
                    while ftr.running():
                        pass
                    
                queue = set(queue) & OrderedSet(progress._tasks.keys())
                queue -= removed
                remaining = remaining - OrderedSet([
                    fid
                    for task, fid in flowTaskMap.items()
                    if task not in progress._tasks.keys()
                    or task in removed
                    ])
                progress.update(main_task, filename=f"Getting TOC ([green]{state['OK']} OK [/green]/[red] {state['KO']} KO[/red])", completed=ndone)
                live.refresh()
                
                
                
                    
def get_toc_for_flow_id(
    client,
    task_id: TaskID,
    flow_id: int,
    dest_path: str,
    state: dict,
    ):  # sourcery skip: extract-method
    """Get the table of contents for a given flow id.
    
    Args:
        client (sdmx.Client): The SDMX client.
        id (int): The dataflow id according to SDMX API spec.
        dest_path (str): The path to the destination file.
        state (dict): The state of the download.

    Returns:
        status (bool): Whether the operation was successful.
        dfvals (pd.DataFrame): The table of contents for the given flow id.
    """
    dfid = client.dataflow(flow_id, params={'references': 'descendants'})
    idobj = sdmx.to_pandas(dfid)
    lengths = [len(val) for k,val in idobj.items()]
    
    lenMap = {}
    nbtasks = len(idobj.keys())
    progress.update(task_id, total=nbtasks)
    
    if done_event.is_set():
        return
    if state["events"][task_id].is_set():
        progress.remove_task(task_id)
        state["KO"] += 1
        return
    
    for k in idobj:
        if state["events"][task_id].is_set():
            progress.remove_task(task_id)
            state["KO"] += 1
            return
        if k in ["structure"]:
            continue
        elif k in ["dataflow"]:
            dataflow_id = idobj[k].reset_index().values[0,0]
            dataflow_name = idobj[k].reset_index().values[0,1]
        elif k in ["codelist"]:
            _currs = [
                pd.DataFrame({
                    "code": [k] * len(v),
                    "code_description": v
                }) for k, v in idobj[k].items()
            ]
            curr = pd.concat(_currs, axis=0)
        else:
            keys = list(idobj[k].keys())
            if len(keys) == 1:
                if isinstance(idobj[k][keys[0]], dict):
                    curr = pd.DataFrame.from_dict(idobj[k][keys[0]])
                elif isinstance(idobj[k][keys[0]], list):
                    curr = pd.DataFrame({"code": idobj[k][keys[0]]})
                elif isinstance(idobj[k][keys[0]], pd.DataFrame):
                    curr = idobj[k][keys[0]]
                elif isinstance(idobj[k][keys[0]], str):
                    curr = pd.DataFrame({"description": [idobj[k][keys[0]]]})
                else:
                    try:
                        curr = pd.DataFrame.from_dict(idobj[k][keys[0]])
                    except Exception as e:
                        return False, pd.DataFrame({"error": [str(e)]})
            else:
                _currs = [
                    pd.DataFrame({
                        "code": [k] * len(v),
                        "code_description": v
                    }) for k, v in idobj[k].items()
                ]
                curr = pd.concat(_currs, axis=0)

        curr.columns = [f"label:{str(col)}" for col in curr.columns]
        curr = curr.reset_index()
        curr = curr.rename(columns={"index":"values"})
        if len(curr) in lenMap:
            lenMap[len(curr)] += [curr]
        else:
            lenMap[len(curr)] = [curr]
        nbtasks += 1
        progress.update(task_id, advance=1, total=nbtasks)
        if done_event.is_set():
            return
    
    if len(lenMap.keys()) == 2:
        progress.update(task_id, filename=f"✅ [green]{dataflow_id} OK[/green]", completed=nbtasks)
        skeys = sorted(lenMap.keys())
        codedefs, codevals = lenMap[skeys[0]], lenMap[skeys[1]]
        dfvals = codevals.pop()
        for df in codevals:
            dfvals = dfvals.merge(df, on="values", how="outer")
        dfvals["dataflow_id"] = dataflow_id
        dfvals["dataflow_name"] = dataflow_name
        # progress.console.log(f"Downloaded {flow_id} successfully", style="green")
        dfvals.to_csv(dest_path, index=False)
        progress.remove_task(task_id)
        state["OK"] += 1
        return True, dfvals, flow_id
    
    elif len(lenMap.keys()) == 3:
        progress.update(task_id, filename=f"✅ [green]{dataflow_id} OK[/green]", completed=nbtasks)
        skeys = sorted(lenMap.keys())
        dfvals = lenMap[skeys[-1]].pop()
        for df in lenMap[skeys[-1]]:
            dfvals = dfvals.merge(df, on="values", how="outer")
        dfvals["dataflow_id"] = dataflow_id
        dfvals["dataflow_name"] = dataflow_name
        progress.remove_task(task_id)
        state["OK"] += 1
        return True, dfvals, flow_id
    
    if state["events"][task_id].is_set():
        progress.remove_task(task_id)
        state["KO"] += 1
        return
    
    progress.update(task_id, filename=f"⚠️ [red]{dataflow_id} KO[/red]", completed=nbtasks)
    dest_path = Path(dest_path)
    log_error(idobj, lenMap, dest_path)
    state["KO"] += 1
    return False, lenMap, flow_id

def log_error(obj, lenMap, path):
    with open(path.with_suffix(".error.log"), "w") as f:
        for k in obj:
            try:
                json.dump(obj[k], f, indent=4)
            except Exception as e:
                pass
        f.write("\n\n")
        f.write("-" * 80)
        f.write("\n\n")
        for l in lenMap:
            f.write(f"Length {l}: {len(lenMap[l])} dataframes\n")
            for df in lenMap[l]:
                try:
                    json.dump((
                                df.to_records()  if isinstance(df, pd.DataFrame)
                                else df.values.tolist() if isinstance(df, pd.Series)
                                else df.tolist() if isinstance(df, (np.ndarray, np.recarray))
                                else df if isinstance(df, (int, float, str, bool))
                                else str(df)
                            ),
                        f,
                        indent=4
                        )
                    f.write("\n\n")
                    f.write("-" * 80)
                    f.write("\n\n")
                except Exception as e:
                    pass

def run():  # sourcery skip: extract-method
    if len(sys.argv) < 3:
        console.print("Usage: python build_toc.py <agency> <dest_dir>", style="red")
        sys.exit(1)
    dest_dir = sys.argv[2]
    agency = sys.argv[1]
    agency = agency.upper()

    if agency not in sdmx.list_sources():
        console.print(f"Agency {agency} not found in SDMX API", style="red")
        sys.exit(1)

    dest_dir = Path(dest_dir)
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
    download_toc(client, flow_ids, dest_dir)