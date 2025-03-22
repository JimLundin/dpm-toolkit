"""Model generation utilities."""

from sqlacodegen.generators import DeclarativeGenerator
from sqlalchemy import Engine, MetaData


def generate_models(metadata: MetaData, engine: Engine) -> str:
    """Generate SQLAlchemy models from database metadata."""
    generator = DeclarativeGenerator(
        metadata,
        engine,
        ["use_inflect", "nojoined", "nobidi"],
        indentation="    ",
        base_class_name="Base",
    )

    return generator.generate()
