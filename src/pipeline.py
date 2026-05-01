import logging  # for pipeline-level log messages
import time     # for measuring how long the full pipeline takes to run

# Import the three main functions — one from each pipeline script
from extract import extract
from transform import transform
from load import load

# Set up logging for the pipeline runner
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Orchestrate the full ETL pipeline.
    Runs extract → transform → load in sequence.
    Logs the time taken for each step and the total pipeline duration.
    """

    logger.info("=" * 60)
    logger.info("PIPELINE START: ecommerce-sales-dbt-pipeline")
    logger.info("=" * 60)

    # Record the start time so we can calculate total duration
    pipeline_start = time.time()

    # --- EXTRACT ---
    logger.info("STEP 1/3: Extract")
    step_start = time.time()
    df_raw = extract()           # calls extract() from extract.py, returns a DataFrame
    step_duration = time.time() - step_start
    # time.time() returns the current time in seconds since the Unix epoch
    # subtracting start from end gives elapsed time in seconds
    logger.info(f"Extract complete in {step_duration:.1f}s — {len(df_raw):,} rows")

    # --- TRANSFORM ---
    logger.info("STEP 2/3: Transform")
    step_start = time.time()
    df_clean = transform(df_raw)  # calls transform() from transform.py
    step_duration = time.time() - step_start
    logger.info(f"Transform complete in {step_duration:.1f}s — {len(df_clean):,} rows remain")

    # --- LOAD ---
    logger.info("STEP 3/3: Load")
    step_start = time.time()
    load(df_clean)               # calls load() from load.py
    step_duration = time.time() - step_start
    logger.info(f"Load complete in {step_duration:.1f}s")

    # --- SUMMARY ---
    total_duration = time.time() - pipeline_start
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {total_duration:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()