from dagster import materialize
from dagster_dbt_demo.definitions import defs


def test_definitions_can_load():
    """Verify that all definitions load without errors."""
    assert defs
    assert defs.get_job_def("etl_job")


def test_resources_configured():
    """Verify that resources are configured."""
    assert "warehouse" in defs.resources
    assert "dbt" in defs.resources


def test_assets_registered():
    """Verify that all expected assets are registered."""
    asset_keys = {key.to_user_string() for key in defs.get_repository_def().get_asset_graph().all_asset_keys}
    expected = {"raw/raw_users", "raw/raw_posts", "raw/raw_comments", "raw/raw_countries"}
    assert expected.issubset(asset_keys), f"Missing assets: {expected - asset_keys}"
