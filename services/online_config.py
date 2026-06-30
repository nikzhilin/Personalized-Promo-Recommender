"""Validated online-store configuration shared by publisher and API."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class OnlineStoreConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    version: str = Field(min_length=1)
    key_prefix: str = Field(min_length=1)
    snapshot_ttl_hours: int = Field(gt=0)
    max_snapshot_bytes: int = Field(gt=0)
    memory_reserve_bytes: int = Field(ge=0)
    memory_overhead_factor: float = Field(ge=1)
    max_top_n: int = Field(gt=0, le=100)
    min_top_n: int = Field(gt=0, le=100)
    fallback_top_n: int = Field(gt=0, le=100)
    pipeline_batch_size: int = Field(gt=0)
    socket_timeout_seconds: float = Field(gt=0)
    required_hdfs_replication: int = Field(gt=0)

    @field_validator("key_prefix")
    @classmethod
    def validate_prefix(cls, value: str) -> str:
        if value.strip() != value or ":" in value:
            raise ValueError("key_prefix must be trimmed and must not contain ':'")
        return value

    @model_validator(mode="after")
    def validate_top_n(self) -> OnlineStoreConfig:
        if self.min_top_n > self.max_top_n:
            raise ValueError("min_top_n must not exceed max_top_n")
        return self

    @property
    def snapshot_ttl_seconds(self) -> int:
        return self.snapshot_ttl_hours * 60 * 60


def load_online_store_config(path: str) -> OnlineStoreConfig:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"online store config does not exist: {source}")
    with source.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise ValueError("online store config root must be a mapping")
    return OnlineStoreConfig.model_validate(payload)
