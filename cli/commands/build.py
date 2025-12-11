from pathlib import Path

import typer
import yaml

from cli.commands.schema import build_database_url, get_sorted_tables, write_config

app = typer.Typer()

@app.command()
def build(
    config_file: Path = typer.Option(
        Path("katcha.yml"),
        "--config", "-c",
        help="Path to katcha.yml config file"
    ),
    default_rows: int = typer.Option(
        10,
        "--rows", "-r",
        help="Default number of rows for new tables"
    ),
):
    """Re-inspect database schema and update katcha.yml with new tables."""
    if not config_file.exists():
        typer.echo(f"Error: Config file not found: {config_file}", err=True)
        typer.echo("Run 'katcha init <database_url>' first.", err=True)
        raise typer.Exit(1)

    with open(config_file) as f:
        config = yaml.safe_load(f)

    if "database" not in config:
        typer.echo("Error: No database configuration found in config file.", err=True)
        typer.echo("Config file may be version 1. Run 'katcha init' again.", err=True)
        raise typer.Exit(1)

    database_url = build_database_url(config["database"])
    typer.echo(f"Inspecting database: {database_url}")

    sorted_tables = get_sorted_tables(database_url)
    existing_schema = config.get("schema", {})

    new_tables = []
    schema = {}
    for table in sorted_tables:
        if table in existing_schema:
            schema[table] = existing_schema[table]
        else:
            schema[table] = default_rows
            new_tables.append(table)

    # Keep tables that were in config but no longer in DB
    removed_tables = []
    for table, rows in existing_schema.items():
        if table not in schema:
            schema[table] = rows
            removed_tables.append(table)

    typer.echo(f"Found {len(sorted_tables)} tables in database")
    if new_tables:
        typer.echo(f"New tables: {', '.join(new_tables)}")
    if removed_tables:
        typer.echo(f"Tables no longer in DB (kept): {', '.join(removed_tables)}")

    schema_changed = new_tables or removed_tables
    if schema_changed:
        config["version"] = config.get("version", 1) + 1
    config["schema"] = schema

    write_config(config, config_file)
    typer.echo(f"Configuration updated: {config_file}")


if __name__ == "__main__":
    app()
