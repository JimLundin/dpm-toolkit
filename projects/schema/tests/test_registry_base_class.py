"""Tests for the DeclarativeMeta registry fix.

The generated base class must provide an explicit ``registry()`` and
``metadata`` so that SQLAlchemy can track mapped classes.  Without
these attributes, importing the generated module raises
``InvalidRequestError``.
"""

from __future__ import annotations

import importlib
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import Column, Date, DateTime, Integer, MetaData, Numeric, String, Table, create_engine

from schema.generation import Model
from schema.main import sqlite_to_schema
from schema.sqlalchemy_export import generate_base_class, schema_to_sqlalchemy


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(name="simple_db")
def simple_database() -> str:
    """Create a minimal SQLite database with one table."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    engine = create_engine(f"sqlite:///{db_path}")
    metadata = MetaData()
    Table(
        "Items",
        metadata,
        Column("ItemID", Integer, primary_key=True),
        Column("Name", String(100)),
    )
    metadata.create_all(engine)
    return db_path


@pytest.fixture(autouse=True)
def _cleanup(simple_db: str) -> Generator[None]:
    yield
    Path(simple_db).unlink(missing_ok=True)


def _ensure_dpm2_stub() -> None:
    """Inject a stub ``dpm2.types`` module if the real package isn't installed.

    The ``schema`` project is deliberately decoupled from ``dpm2`` — ``dpm2``
    consumes schema's output, not the other way round. But generated code
    imports ``from dpm2.types import DPMDate, DPMDateTime`` so that the
    runtime registry can wrap dates. For the schema tests we only care that
    the generated code is syntactically/semantically importable, so a stub
    with the two expected names is sufficient.
    """
    if "dpm2.types" in sys.modules:
        return
    try:  # pragma: no cover - exercised only when dpm2 is installed
        import dpm2.types  # noqa: F401, PLC0415
    except ImportError:
        from sqlalchemy import String  # noqa: PLC0415
        from sqlalchemy.types import TypeDecorator  # noqa: PLC0415

        dpm2_pkg = ModuleType("dpm2")
        dpm2_types = ModuleType("dpm2.types")

        class _StubDPMDate(TypeDecorator):  # type: ignore[type-arg]
            impl = String
            cache_ok = True

        class _StubDPMDateTime(TypeDecorator):  # type: ignore[type-arg]
            impl = String
            cache_ok = True

        dpm2_types.DPMDate = _StubDPMDate  # type: ignore[attr-defined]
        dpm2_types.DPMDateTime = _StubDPMDateTime  # type: ignore[attr-defined]
        sys.modules["dpm2"] = dpm2_pkg
        sys.modules["dpm2.types"] = dpm2_types


def _import_generated_code(code: str, module_name: str) -> ModuleType:
    """Write generated code to a temp file and import it as a real module.

    This ensures ``from __future__ import annotations`` resolves correctly
    and SQLAlchemy can find ``Mapped`` in the module's namespace.
    """
    _ensure_dpm2_stub()

    with tempfile.NamedTemporaryFile(
        suffix=".py", delete=False, mode="w"
    ) as tmp:
        tmp.write(code)
        tmp_path = tmp.name

    spec = importlib.util.spec_from_file_location(module_name, tmp_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)
        Path(tmp_path).unlink(missing_ok=True)
    return module


# ---------------------------------------------------------------------------
# Unit tests – generate_base_class (sqlalchemy_export)
# ---------------------------------------------------------------------------


class TestGenerateBaseClass:
    """Tests for the standalone ``generate_base_class`` helper."""

    def test_contains_registry_instantiation(self) -> None:
        code = generate_base_class("DPM")
        assert "_registry = registry()" in code

    def test_contains_abstract_marker(self) -> None:
        code = generate_base_class("DPM")
        assert "__abstract__ = True" in code

    def test_contains_registry_assignment(self) -> None:
        code = generate_base_class("DPM")
        assert "registry = _registry" in code

    def test_contains_metadata_assignment(self) -> None:
        code = generate_base_class("DPM")
        assert "metadata = _registry.metadata" in code

    def test_uses_declarative_meta(self) -> None:
        code = generate_base_class("DPM")
        assert "metaclass=DeclarativeMeta" in code

    def test_custom_base_name(self) -> None:
        code = generate_base_class("Base")
        assert "class Base(metaclass=DeclarativeMeta):" in code

    def test_no_type_annotation_map_when_no_dates(self) -> None:
        """Without DPM date types in imports the registry() call stays empty."""
        code = generate_base_class("DPM", imports={})
        assert "registry()" in code
        assert "type_annotation_map" not in code

    def test_emits_type_annotation_map_for_dpm_date(self) -> None:
        """DPMDate usage should register a date → DPMDate mapping."""
        imports: dict[str, set[str]] = {"dpm2.types": {"DPMDate"}}
        code = generate_base_class("DPM", imports=imports)
        assert "type_annotation_map={datetime.date: DPMDate}" in code
        # datetime import must be recorded so the generated module can use it.
        assert "datetime" in imports

    def test_emits_type_annotation_map_for_dpm_datetime(self) -> None:
        """DPMDateTime usage should register a datetime → DPMDateTime mapping."""
        imports: dict[str, set[str]] = {"dpm2.types": {"DPMDateTime"}}
        code = generate_base_class("DPM", imports=imports)
        assert "datetime.datetime: DPMDateTime" in code

    def test_emits_combined_type_annotation_map(self) -> None:
        """Both date types can coexist in the same type_annotation_map."""
        imports: dict[str, set[str]] = {"dpm2.types": {"DPMDate", "DPMDateTime"}}
        code = generate_base_class("DPM", imports=imports)
        assert "datetime.date: DPMDate" in code
        assert "datetime.datetime: DPMDateTime" in code


# ---------------------------------------------------------------------------
# Unit tests – Model._generate_base_class (generation.py)
# ---------------------------------------------------------------------------


class TestModelGenerateBaseClass:
    """Tests for the class-based ``Model._generate_base_class`` method."""

    def test_contains_registry_instantiation(self) -> None:
        model = Model(MetaData())
        code = model._generate_base_class()
        assert "_registry = registry()" in code

    def test_contains_abstract_marker(self) -> None:
        model = Model(MetaData())
        code = model._generate_base_class()
        assert "__abstract__ = True" in code

    def test_contains_registry_assignment(self) -> None:
        model = Model(MetaData())
        code = model._generate_base_class()
        assert "registry = _registry" in code

    def test_contains_metadata_assignment(self) -> None:
        model = Model(MetaData())
        code = model._generate_base_class()
        assert "metadata = _registry.metadata" in code

    def test_adds_registry_import(self) -> None:
        model = Model(MetaData())
        model._generate_base_class()
        assert "registry" in model.imports["sqlalchemy.orm"]
        assert "DeclarativeMeta" in model.imports["sqlalchemy.orm"]


# ---------------------------------------------------------------------------
# Integration – generated code can be imported without InvalidRequestError
# ---------------------------------------------------------------------------


class TestGeneratedCodeImportable:
    """Verify that generated models can be imported as a real Python module.

    Without the registry fix the import raises
    ``sqlalchemy.exc.InvalidRequestError``.
    """

    def test_schema_to_sqlalchemy_importable(self, simple_db: str) -> None:
        """Generated code from schema_to_sqlalchemy should import cleanly."""
        engine = create_engine(f"sqlite:///{simple_db}")
        schema = sqlite_to_schema(engine)
        code = schema_to_sqlalchemy(schema)

        module = _import_generated_code(code, "_test_registry_schema")
        assert hasattr(module, "DPM")
        assert hasattr(module, "Items")

    def test_full_schema_imports_registry(self, simple_db: str) -> None:
        """Generated code should import both DeclarativeMeta and registry."""
        engine = create_engine(f"sqlite:///{simple_db}")
        schema = sqlite_to_schema(engine)
        code = schema_to_sqlalchemy(schema)

        assert "from sqlalchemy.orm import" in code
        assert "DeclarativeMeta" in code
        assert "registry" in code


# ---------------------------------------------------------------------------
# Integration – date/datetime/numeric types resolve at runtime
# ---------------------------------------------------------------------------


@pytest.fixture(name="typed_db")
def typed_database() -> Generator[str]:
    """Create a SQLite database with Date, DateTime, and Numeric columns."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    engine = create_engine(f"sqlite:///{db_path}")
    metadata = MetaData()
    Table(
        "Events",
        metadata,
        Column("EventID", Integer, primary_key=True),
        Column("Name", String(100)),
        Column("EventDate", Date),
        Column("CreatedAt", DateTime),
        Column("Price", Numeric(10, 2)),
    )
    metadata.create_all(engine)
    yield db_path
    Path(db_path).unlink(missing_ok=True)


class TestDateTimeTypesImportable:
    """Verify that generated models with date/datetime/numeric types can be imported.

    Previously these types were placed inside ``if TYPE_CHECKING:`` and were
    unavailable at runtime, causing ``MappedAnnotationError``.
    """

    def test_generation_model_importable(self, typed_db: str) -> None:
        """Model.render() output should import without MappedAnnotationError."""
        engine = create_engine(f"sqlite:///{typed_db}")
        metadata = MetaData()
        metadata.reflect(bind=engine)
        code = Model(metadata).render()

        module = _import_generated_code(code, "_test_generation_dates")
        assert hasattr(module, "Events")

    def test_schema_to_sqlalchemy_importable(self, typed_db: str) -> None:
        """schema_to_sqlalchemy output should import without MappedAnnotationError."""
        engine = create_engine(f"sqlite:///{typed_db}")
        schema = sqlite_to_schema(engine)
        code = schema_to_sqlalchemy(schema)

        module = _import_generated_code(code, "_test_schema_dates")
        assert hasattr(module, "Events")
