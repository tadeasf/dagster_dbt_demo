import pandas as pd
import requests
from dagster import AssetExecutionContext, asset

from dagster_dbt_demo.resources.database import WarehouseResource


@asset(key_prefix=["raw"], group_name="ingestion")
def raw_users(context: AssetExecutionContext, warehouse: WarehouseResource) -> None:
    """Ingest users from JSONPlaceholder API into raw schema."""
    response = requests.get("https://jsonplaceholder.typicode.com/users", timeout=30)
    response.raise_for_status()
    users = response.json()

    rows = []
    for u in users:
        rows.append({
            "id": u["id"],
            "name": u["name"],
            "username": u["username"],
            "email": u["email"],
            "phone": u["phone"],
            "website": u["website"],
            "company_name": u["company"]["name"],
            "company_bs": u["company"]["bs"],
            "city": u["address"]["city"],
            "street": u["address"]["street"],
            "zipcode": u["address"]["zipcode"],
            "lat": u["address"]["geo"]["lat"],
            "lng": u["address"]["geo"]["lng"],
        })

    df = pd.DataFrame(rows)
    engine = warehouse.get_engine()
    df.to_sql("raw_users", engine, schema="raw", if_exists="replace", index=False)
    context.log.info(f"Loaded {len(df)} users into raw.raw_users")


@asset(key_prefix=["raw"], group_name="ingestion")
def raw_posts(context: AssetExecutionContext, warehouse: WarehouseResource) -> None:
    """Ingest posts from JSONPlaceholder API into raw schema."""
    response = requests.get("https://jsonplaceholder.typicode.com/posts", timeout=30)
    response.raise_for_status()

    df = pd.DataFrame(response.json())
    engine = warehouse.get_engine()
    df.to_sql("raw_posts", engine, schema="raw", if_exists="replace", index=False)
    context.log.info(f"Loaded {len(df)} posts into raw.raw_posts")


@asset(key_prefix=["raw"], group_name="ingestion")
def raw_comments(context: AssetExecutionContext, warehouse: WarehouseResource) -> None:
    """Ingest comments from JSONPlaceholder API into raw schema."""
    response = requests.get("https://jsonplaceholder.typicode.com/comments", timeout=30)
    response.raise_for_status()

    df = pd.DataFrame(response.json())
    engine = warehouse.get_engine()
    df.to_sql("raw_comments", engine, schema="raw", if_exists="replace", index=False)
    context.log.info(f"Loaded {len(df)} comments into raw.raw_comments")


@asset(key_prefix=["raw"], group_name="ingestion")
def raw_countries(context: AssetExecutionContext, warehouse: WarehouseResource) -> None:
    """Ingest country data from REST Countries API into raw schema."""
    fields = "name,cca2,cca3,region,subregion,population,area,capital,latlng,languages"
    response = requests.get(
        f"https://restcountries.com/v3.1/all?fields={fields}", timeout=60
    )
    response.raise_for_status()
    countries = response.json()

    rows = []
    for c in countries:
        latlng = c.get("latlng", [None, None])
        capitals = c.get("capital", [])
        languages = c.get("languages", {})

        rows.append({
            "name_common": c.get("name", {}).get("common"),
            "name_official": c.get("name", {}).get("official"),
            "cca2": c.get("cca2"),
            "cca3": c.get("cca3"),
            "region": c.get("region"),
            "subregion": c.get("subregion"),
            "population": c.get("population"),
            "area_sq_km": c.get("area"),
            "capital": capitals[0] if capitals else None,
            "lat": latlng[0] if len(latlng) > 0 else None,
            "lng": latlng[1] if len(latlng) > 1 else None,
            "languages": ", ".join(languages.values()),
        })

    df = pd.DataFrame(rows)
    engine = warehouse.get_engine()
    df.to_sql("raw_countries", engine, schema="raw", if_exists="replace", index=False)
    context.log.info(f"Loaded {len(df)} countries into raw.raw_countries")
