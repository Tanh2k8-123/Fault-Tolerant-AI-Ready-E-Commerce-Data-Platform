"""Configuration shared by the generator and the MinIO uploader."""

import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class FaultRates:
    duplicate: Decimal = Decimal("0.03")
    null_key: Decimal = Decimal("0.01")
    negative: Decimal = Decimal("0.005")
    late: Decimal = Decimal("0.05")
    pii: Decimal = Decimal("0.02")

    def validate(self) -> None:
        order_rates = (self.duplicate, self.null_key, self.negative, self.late)
        if any(not r.is_finite() or not 0 <= r <= 1 for r in (*order_rates, self.pii)):
            raise ValueError(
                "Every fault rate must be a finite decimal between 0 and 1"
            )
        if sum(order_rates) > 1:
            raise ValueError("Disjoint order fault rates cannot sum to more than 1")


@dataclass(frozen=True)
class GeneratorConfig:
    random_seed: int = 42
    batch_id: str = "demo01"
    start_date: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    num_days: int = 30
    num_orders: int = 10_000
    num_customers: int = 1_000
    num_products: int = 100
    num_reviews: int = 1_000
    shard_size: int = 250_000
    rates: FaultRates = field(default_factory=FaultRates)

    def __post_init__(self) -> None:
        self.rates.validate()
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", self.batch_id):
            raise ValueError(
                "batch-id must contain only letters, digits, underscores or hyphens"
            )
        if self.start_date.utcoffset() != UTC.utcoffset(None) or any(
            (
                self.start_date.hour,
                self.start_date.minute,
                self.start_date.second,
                self.start_date.microsecond,
            )
        ):
            raise ValueError("start_date must be midnight UTC")
        if (
            self.random_seed < 0
            or min(
                self.num_days,
                self.num_orders,
                self.num_customers,
                self.num_products,
                self.shard_size,
            )
            < 1
        ):
            raise ValueError(
                "Seed must be nonnegative and all sizes except reviews must be positive"
            )
        eligible = (
            self.num_orders
            - int(self.rates.null_key * self.num_orders)
            - int(self.rates.negative * self.num_orders)
        )
        if not 0 <= self.num_reviews <= eligible:
            raise ValueError("Review count exceeds eligible orders or is negative")


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    bucket: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)

    @classmethod
    def from_env(cls) -> "MinioConfig":
        load_dotenv(Path.cwd() / ".env", override=False)
        user = os.getenv("MINIO_ROOT_USER")
        password = os.getenv("MINIO_ROOT_PASSWORD")
        if not user or not password:
            raise ValueError(
                "Set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD in the environment or .env"
            )
        return cls(
            os.getenv("MINIO_ENDPOINT_URL", "http://127.0.0.1:9000"),
            os.getenv("MINIO_BUCKET", "ecommerce"),
            user,
            password,
        )
