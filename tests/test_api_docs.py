"""docs/openapi.json and the endpoint tables in docs/api-reference.md are generated from the
app. This fails when a route changes (or is added without a description) and the generated
files weren't regenerated."""

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "export_openapi.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("export_openapi", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generated_api_docs_are_current():
    module = _load_script()
    openapi_text, reference = module.build()
    assert module.OPENAPI_PATH.read_text() == openapi_text, (
        "docs/openapi.json is stale; run: DB_PASSWORD=test python scripts/export_openapi.py"
    )
    assert module.REFERENCE_PATH.read_text() == reference, (
        "docs/api-reference.md is stale; run: DB_PASSWORD=test python scripts/export_openapi.py"
    )
