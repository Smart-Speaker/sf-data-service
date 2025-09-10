#!/usr/bin/env python3
import sys
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
import locale
import os
import io
import re
from typing import List

# =======================
# Config via ENV (no INI)
# =======================
# Folder containing the child scripts (relative to this file)
SCRIPTS_DIRNAME = os.environ.get("RUNNER_SCRIPTS_DIR", "files/scripts")

# Space/comma/semicolon-separated list of scripts to run in order
# e.g. "app.py distributer.py"
SCRIPTS_LIST = os.environ.get("RUNNER_SCRIPTS", "app.py distributer.py")

# Log rollover config
MAX_LOG_BYTES = int(os.environ.get("RUNNER_MAX_LOG_BYTES", str(20 * 1024 * 1024)))  # 20MB

# Child process output handling
SUBPROCESS_ENCODING = "utf-8"
SUBPROCESS_ERRORS = "replace"

PYTHON_EXE = sys.executable

# Make console tolerant to Unicode
try:
    sys.stdout.reconfigure(encoding=locale.getpreferredencoding(False) or "utf-8", errors="replace")
except Exception:
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer,
        encoding=locale.getpreferredencoding(False) or "utf-8",
        errors="replace",
    )

# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
ARCHIVE_DIR = LOG_DIR / "archive"
SCRIPTS_DIR = BASE_DIR / SCRIPTS_DIRNAME

def ensure_dirs():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Logging with UTC date + size rotation ----------
def _log_filename_for(date_utc: datetime.date, part_index: int) -> Path:
    stamp = date_utc.strftime("%Y%m%d")
    suffix = "" if part_index == 0 else f"-{part_index}"
    return LOG_DIR / f"runner-{stamp}{suffix}.log"

class UTCDateAndSizeRotatingFileHandler(logging.FileHandler):
    def __init__(self, max_bytes: int):
        self.max_bytes = max_bytes
        self.current_date = datetime.now(timezone.utc).date()
        self.part_index = self._find_next_part_index(self.current_date)
        self._update_basefilename()
        super().__init__(self.baseFilename, encoding="utf-8")

    def _update_basefilename(self):
        self.baseFilename = str(_log_filename_for(self.current_date, self.part_index))

    def _find_next_part_index(self, date_utc):
        idx = 0
        while True:
            p = _log_filename_for(date_utc, idx)
            if not p.exists():
                return idx
            try:
                if p.stat().st_size < self.max_bytes:
                    return idx
            except Exception:
                pass
            idx += 1

    def _should_rollover(self) -> bool:
        now_date = datetime.now(timezone.utc).date()
        if now_date != self.current_date:
            return True
        try:
            self.stream.flush()
        except Exception:
            pass
        try:
            size = self.stream.tell() if self.stream and self.stream.seekable() else os.path.getsize(self.baseFilename)
        except Exception:
            size = 0
        return size >= self.max_bytes

    def _do_rollover(self):
        now_date = datetime.now(timezone.utc).date()
        if now_date != self.current_date:
            self.current_date = now_date
            self.part_index = 0
        else:
            self.part_index += 1
        self.acquire()
        try:
            if self.stream:
                self.stream.close()
                self.stream = None
            self._update_basefilename()
            self.stream = self._open()
        finally:
            self.release()

    def emit(self, record):
        if self._should_rollover():
            self._do_rollover()
        super().emit(record)

_logger_singleton: logging.Logger | None = None
def get_logger() -> logging.Logger:
    global _logger_singleton
    if _logger_singleton is not None:
        return _logger_singleton
    ensure_dirs()
    logger = logging.getLogger("runner")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fhandler = UTCDateAndSizeRotatingFileHandler(MAX_LOG_BYTES)
    fhandler.setFormatter(fmt)
    logger.addHandler(fhandler)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    _logger_singleton = logger
    return logger

def housekeeping(logger: logging.Logger):
    now = datetime.now()
    for p in LOG_DIR.glob("runner-*.log"):
        try:
            if not p.is_file():
                continue
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            if now - mtime > timedelta(days=1):
                today_utc_stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
                if today_utc_stamp in p.name:
                    continue
                archived = ARCHIVE_DIR / p.name
                p.replace(archived)
                logger.info(f"Archived log: {p.name} -> archive/{archived.name}")
        except Exception as e:
            logger.warning(f"Housekeeping: failed to archive {p.name}: {e}")

    for p in ARCHIVE_DIR.glob("runner-*.log"):
        try:
            if not p.is_file():
                continue
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            if now - mtime > timedelta(days=30):
                p.unlink(missing_ok=True)
                logger.info(f"Deleted archived log (30+ days): archive/{p.name}")
        except Exception as e:
            logger.warning(f"Housekeeping: failed to delete archive/{p.name}: {e}")

# ---------- Helpers ----------
def parse_scripts_list(raw: str) -> List[str]:
    parts = re.split(r"[,\s;]+", raw.strip())
    return [p for p in parts if p]

def run_script(script_path: Path, logger: logging.Logger) -> int:
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return -1
    cwd = script_path.parent
    logger.info(f"Starting script: {script_path.name} (cwd={cwd})")
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    try:
        proc = subprocess.Popen(
            [PYTHON_EXE, str(script_path)],
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding=SUBPROCESS_ENCODING,
            errors=SUBPROCESS_ERRORS,
            env=env,
        )
    except Exception as e:
        logger.error(f"Failed to start {script_path.name}: {e}")
        return -1

    start = datetime.now()
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            logger.info(f"[{script_path.name}] {line.rstrip()}")
    except Exception as e:
        logger.error(f"Error reading output from {script_path.name}: {e}")

    proc.wait()
    dur = (datetime.now() - start).total_seconds()
    logger.info(f"Finished {script_path.name}: rc={proc.returncode} in {dur:.1f}s")
    return proc.returncode

# ---------- Public entry: run ONE cycle ----------
def run_one_cycle():
    logger = get_logger()

    script_names = parse_scripts_list(SCRIPTS_LIST)
    if not script_names:
        logger.error("RUNNER_SCRIPTS env var is empty; nothing to run.")
        return

    logger.info(f"Scripts dir  : {SCRIPTS_DIR}")
    logger.info(f"Scripts list : {script_names}")

    cycle_start = datetime.now(timezone.utc)
    logger.info("Cycle starting.")
    logger.info(f"Using Python : {PYTHON_EXE}")
    logger.info(f"UTC now      : {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")

    housekeeping(logger)

    for idx, name in enumerate(script_names):
        script_path = SCRIPTS_DIR / name
        logger.info(f"--- [{idx}] Running {name} ---")
        rc = run_script(script_path, logger)
        if rc != 0:
            logger.warning(f"Script {name} exited with rc={rc}")

    cycle_dur = (datetime.now(timezone.utc) - cycle_start).total_seconds()
    logger.info(f"Cycle finished in {cycle_dur:.1f}s")

def main():
    run_one_cycle()

if __name__ == "__main__":
    main()
