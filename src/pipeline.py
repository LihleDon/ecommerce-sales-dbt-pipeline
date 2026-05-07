import logging
import time

from extract import extract
from transform import transform
from load import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Orchestrate the full ETL pipeline: Extract, Transform, Load.
    Each step is timed individually and the total duration is logged at the end.
    """

    logger.info("=" * 60)
    logger.info("PIPELINE START: ecommerce-sales-dbt-pipeline")
    logger.info("=" * 60)

    pipeline_start = time.time()

    logger.info("STEP 1/3: Extract")
    step_start = time.time()
    df_raw = extract()
    logger.info(f"Extract complete in {time.time() - step_start:.1f}s — {len(df_raw):,} rows")

    logger.info("STEP 2/3: Transform")
    step_start = time.time()
    df_clean = transform(df_raw)
    logger.info(f"Transform complete in {time.time() - step_start:.1f}s — {len(df_clean):,} rows remain")

    logger.info("STEP 3/3: Load")
    step_start = time.time()
    load(df_clean)
    logger.info(f"Load complete in {time.time() - step_start:.1f}s")

    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {time.time() - pipeline_start:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
