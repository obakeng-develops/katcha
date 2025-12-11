from collections import defaultdict
from graphlib import TopologicalSorter
from pathlib import Path

import typer
import yaml
from sqlalchemy import create_engine, inspect

app = typer.Typer()

def build_dependency_graph(inspector) -> dict[str, set[str]]:
    """Build a dependency graph from foreign key relationships.

    Returns a dict mapping table_name -> set of tables it depends on.
    """
    graph = defaultdict(set)
    tables = inspector.get_table_names()
    
    for table in tables:
        graph[table]  # Ensure all tables are in the graph
        foreign_keys = inspector.get_foreign_keys(table)
        for fk in foreign_keys:
            referred_table = fk["referred_table"]
            if referred_table and referred_table != table:  # Skip self-references
                graph[table].add(referred_table)

    return dict(graph)


def topological_sort(graph: dict[str, set[str]]) -> list[str]:
    """Sort tables topologically based on their dependencies.

    Tables with no dependencies come first, followed by tables
    that depend on them.
    """
    sorter = TopologicalSorter(graph)
    return list(sorter.static_order())


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
    engine = create_engine(database_url)
    inspector = inspect(engine)

    typer.echo(f"Inspecting database: {database_url}")

    graph = build_dependency_graph(inspector)
    sorted_tables = topological_sort(graph)

    typer.echo(f"Found {len(sorted_tables)} tables")

    schema = {table: default_rows for table in sorted_tables}

    config = {
        "version": 1,
        "schema": schema,
    }

    with open(output, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    typer.echo(f"Configuration written to {output}")


if __name__ == "__main__":
    app()
