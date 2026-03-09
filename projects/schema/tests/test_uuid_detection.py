"""Tests for UUID column type detection during reflection and code generation."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Uuid,
    create_engine,
    event,
)

from schema.main import detect_types, sqlite_to_schema
from schema.sqlalchemy_export import schema_to_sqlalchemy


@pytest.fixture(name="guid_db")
def database_with_guids() -> Generator[str]:
    """Create a SQLite database mimicking DPM structure with GUID columns."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    engine = create_engine(f"sqlite:///{db_path}")
    metadata = MetaData()

    Table(
        "Concept",
        metadata,
        Column("ConceptGUID", String, primary_key=True),
        Column("Name", String(100)),
    )

    Table(
        "Organisation",
        metadata,
        Column("OrganisationID", Integer, primary_key=True),
        Column("RowGUID", String, ForeignKey("Concept.ConceptGUID")),
        Column("Name", String(100)),
    )

    Table(
        "Metric",
        metadata,
        Column("MetricID", Integer, primary_key=True),
        Column("RowGUID", String),
        Column("Description", String(200)),
    )

    metadata.create_all(engine)
    yield db_path
    Path(db_path).unlink(missing_ok=True)


class TestUuidDetectionDuringReflection:
    """Verify that GUID columns are detected as Uuid type during reflection."""

    def test_concept_guid_detected_as_uuid(self, guid_db: str) -> None:
        engine = create_engine(f"sqlite:///{guid_db}")
        metadata = MetaData()
        event.listen(metadata, "column_reflect", detect_types)
        metadata.reflect(bind=engine)

        concept = metadata.tables["Concept"]
        assert isinstance(concept.columns["ConceptGUID"].type, Uuid)

    def test_row_guid_detected_as_uuid(self, guid_db: str) -> None:
        engine = create_engine(f"sqlite:///{guid_db}")
        metadata = MetaData()
        event.listen(metadata, "column_reflect", detect_types)
        metadata.reflect(bind=engine)

        org = metadata.tables["Organisation"]
        assert isinstance(org.columns["RowGUID"].type, Uuid)

    def test_non_guid_columns_unchanged(self, guid_db: str) -> None:
        engine = create_engine(f"sqlite:///{guid_db}")
        metadata = MetaData()
        event.listen(metadata, "column_reflect", detect_types)
        metadata.reflect(bind=engine)

        org = metadata.tables["Organisation"]
        assert not isinstance(org.columns["OrganisationID"].type, Uuid)
        assert not isinstance(org.columns["Name"].type, Uuid)


class TestUuidCodeGeneration:
    """Verify that generated code uses uuid.UUID for GUID columns."""

    def test_schema_to_sqlalchemy_uses_uuid(self, guid_db: str) -> None:
        engine = create_engine(f"sqlite:///{guid_db}")
        schema = sqlite_to_schema(engine)
        code = schema_to_sqlalchemy(schema)

        assert "import uuid" in code
        assert "concept_guid: Mapped[uuid.UUID]" in code

    def test_row_guid_fk_uses_uuid(self, guid_db: str) -> None:
        engine = create_engine(f"sqlite:///{guid_db}")
        schema = sqlite_to_schema(engine)
        code = schema_to_sqlalchemy(schema)

        assert "row_guid: Mapped[uuid.UUID | None]" in code

    def test_non_guid_columns_remain_str(self, guid_db: str) -> None:
        engine = create_engine(f"sqlite:///{guid_db}")
        schema = sqlite_to_schema(engine)
        code = schema_to_sqlalchemy(schema)

        assert "name: Mapped[str | None]" in code
        assert "description: Mapped[str | None]" in code
