import polars as pl
import threading
import pathlib

_conj_lock = threading.Lock()
_conj = pl.read_parquet(pathlib.Path(__file__).parent.resolve() / "conj.parquet")


def _get_conjunctions() -> pl.DataFrame:
    with _conj_lock:
        return _conj
