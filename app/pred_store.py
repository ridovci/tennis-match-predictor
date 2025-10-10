import os
import json
from pathlib import Path
from contextlib import contextmanager
from typing import Dict


BASE_DIR = Path(__file__).resolve().parent


def pred_dir_for(date_str: str) -> Path:
    base = BASE_DIR.parent / "data" / "predictions"
    base.mkdir(parents=True, exist_ok=True)
    return base


def pred_file_for(date_str: str) -> Path:
    return pred_dir_for(date_str) / f"{date_str}.json"


def _atomic_write_json(path: Path, data_obj: dict):
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data_obj, f, ensure_ascii=False, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


@contextmanager
def _file_lock(lock_path: Path):
    import time
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            break
        except FileExistsError:
            time.sleep(0.05)
    try:
        yield
    finally:
        try:
            os.unlink(str(lock_path))
        except FileNotFoundError:
            pass


def read_predictions(date_str: str) -> Dict[str, dict]:
    p = pred_file_for(date_str)
    if not p.exists():
        return {}
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            return {}
    except Exception:
        return {}


def write_predictions(date_str: str, data_obj: Dict[str, dict]):
    p = pred_file_for(date_str)
    lock = p.with_suffix(".lock")
    with _file_lock(lock):
        _atomic_write_json(p, data_obj)


