import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path


PROCESSED_DIR = Path("data/processed")

DB_URL = "postgresql://admin:admin123@host.docker.internal:5432/ecommerce"

engine = create_engine(DB_URL)


def load_file(file_name, table_name):
    file_path = PROCESSED_DIR / file_name

    df = pd.read_csv(file_path)

    df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False
    )

    print(f"Loaded {len(df)} rows into {table_name}")


def main():

    load_file("customers.csv", "customers")
    load_file("products.csv", "products")
    load_file("orders.csv", "orders")
    load_file("payments.csv", "payments")

    print("All data loaded successfully!")


if __name__ == "__main__":
    main()