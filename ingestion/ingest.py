import pandas as pd
from pathlib import Path
import logging
from datetime import datetime


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def ingest_file(file_path):
    try:
        logging.info(f"Reading file: {file_path}")

        df = pd.read_csv(file_path)

        output_path = PROCESSED_DIR / file_path.name
        df.to_csv(output_path, index=False)

        logging.info(
            f"Successfully processed {file_path.name} - {len(df)} rows"
        )

    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        raise


def main():
    start_time = datetime.now()

    logging.info("Starting data ingestion")

    csv_files = list(RAW_DIR.glob("*.csv"))

    if not csv_files:
        logging.warning("No CSV files found")
        return

    for file_path in csv_files:
        ingest_file(file_path)

    end_time = datetime.now()

    logging.info(
        f"Ingestion completed in {end_time - start_time}"
    )


if __name__ == "__main__":
    main()