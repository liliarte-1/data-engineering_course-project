# src/orchestration.py
import logging
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

# Paths 
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
LOGS = ROOT / "logs"
WAREHOUSE = ROOT / "warehouse"

# Commands
INGESTION = [sys.executable, SRC / "ingestion.py"]
TRANSFORMATION = [sys.executable, SRC / "transformation.py"]
LOAD_DW = [sys.executable, SRC / "load_dw.py"]
SCHEMA = ["psql", "-f", WAREHOUSE / "schema.sql"]


# Retry policy for ingestion
INGESTION_RETRIES = 3
BACKOFF = 2


# logging
logging.basicConfig(
    filename=LOGS / "orchestration.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)
logger.info(f"Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def run(cmd, step_name, retry=False):
    attempts = INGESTION_RETRIES if retry else 1

    for attempt in range(1, attempts + 1):
        logger.info(f"[{step_name}] Running (attempt {attempt}/{attempts})")

        result = subprocess.run(cmd)

        if result.returncode == 0:
            logger.info(f"[{step_name}] Completed successfully")
            return True

        logger.error(f"[{step_name}] Failed with return code {result.returncode}")

        if not retry:
            return False

        time.sleep(BACKOFF * attempt)

    logger.error(f"[{step_name}] Exhausted retries")
    return False


def run_pipeline():
    # ingestion → retry
    if not run(INGESTION, "ingestion", retry=True):
        logger.error("Pipeline stopped at ingestion step")
        sys.exit(1)

    # transformation → stop
    if not run(TRANSFORMATION, "transformation"):
        logger.error("Pipeline stopped at transformation step")
        sys.exit(2)

    # not necessary if there if tables are already created
    # # schema → stop
    # if not run(SCHEMA, "schema"):
    #     logger.error("Pipeline stopped at schema step")
    #     sys.exit(3)

    # load_dw → stop
    if not run(LOAD_DW, "load_dw"):
        logger.error("Pipeline stopped at load_dw step")
        sys.exit(4)

    logger.info("Pipeline finished successfully")
