"""Tests for the published dpm-toolkit package interface."""

from importlib.metadata import requires
from importlib.resources import files
from re import split

import pytest

INTERNAL_MODULES = (
    "analysis",
    "archive",
    "compare",
    "migrate",
    "schema",
    "scrape",
)

# Non-Python files each internal module loads at runtime. These are only
# reachable if the build backend ships them inside the wheel.
BUNDLED_DATA_FILES = (
    "analysis/templates/report.md",
    "archive/versions.toml",
    "compare/templates/report.html",
    "compare/templates/scripts.js",
    "compare/templates/styles.css",
    "schema/templates/diagram.css",
    "schema/templates/diagram.html",
    "schema/templates/diagram.js",
)


def test_internal_modules_are_not_distribution_dependencies() -> None:
    """Internal modules must be bundled rather than resolved from PyPI."""
    requirements = requires("dpm-toolkit") or []
    dependency_names = {
        split(r"[ <>=!~\[]", requirement, maxsplit=1)[0].lower()
        for requirement in requirements
    }

    assert set(INTERNAL_MODULES).isdisjoint(dependency_names)


@pytest.mark.parametrize("module", INTERNAL_MODULES)
def test_internal_module_is_bundled_in_the_package(module: str) -> None:
    """Every internal module must be importable as a dpm_toolkit subpackage."""
    assert files("dpm_toolkit").joinpath(f"{module}/__init__.py").is_file()


@pytest.mark.parametrize("data_file", BUNDLED_DATA_FILES)
def test_runtime_data_files_are_bundled(data_file: str) -> None:
    """Templates and version metadata must ship alongside the code."""
    assert files("dpm_toolkit").joinpath(data_file).is_file()
