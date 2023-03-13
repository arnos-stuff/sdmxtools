from rich.console import Console
from pathlib import Path

from rich.progress import (
    BarColumn,
    TimeElapsedColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
    SpinnerColumn,
)

__all__ = ["console", "dlProgress", "TaskID"]

console = Console()

dlProgress = Progress(
    SpinnerColumn(),
    TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    MofNCompleteColumn(),
    "•",
    TimeElapsedColumn(),
    "•",
    TimeRemainingColumn(),
    auto_refresh=True,
    refresh_per_second=5,
)