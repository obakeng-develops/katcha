from pathlib import Path

import typer

from cli.commands.schema import get_sorted_tables, parse_database_url, write_config

app = typer.Typer()


@app.command()
def init(
    database_url: str = typer.Argument(..., help="Database connection URL"),
    output: Path = typer.Option(
        Path("katcha.yml"),
        "--output", "-o",
        help="Output file path"
    ),
    default_rows: int = typer.Option(
        10,
        "--rows", "-r",
        help="Default number of rows per table"
    ),
):
    """Initialize katcha configuration by inspecting a database schema."""
    typer.echo(f"Inspecting database: {database_url}")

    sorted_tables = get_sorted_tables(database_url)
    typer.echo(f"Found {len(sorted_tables)} tables")

    schema = {table: default_rows for table in sorted_tables}
    db_config = parse_database_url(database_url)

    config = {
        "version": 1,
        "database": db_config,
        "schema": schema,
    }

    write_config(config, output)
    typer.echo(f"Configuration written to {output}")


if __name__ == "__main__":
    app()
