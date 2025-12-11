from collections import defaultdict
from graphlib import TopologicalSorter
from urllib.parse import urlparse

from sqlalchemy import create_engine, inspect


def parse_database_url(database_url: str) -> dict:
    """Parse a database URL into engine and host components."""
    parsed = urlparse(database_url)
    engine = parsed.scheme.split("+")[0]  # Handle dialect+driver format

    if engine == "sqlite":
        # For SQLite, host is the file path
        host = parsed.path.lstrip("/") or parsed.netloc
    else:
        host = parsed.netloc

    return {"engine": engine, "host": host}


def build_database_url(db_config: dict) -> str:
    """Build a database URL from engine and host config."""
    engine = db_config["engine"]
    host = db_config["host"]

    if engine == "sqlite":
        return f"sqlite:///{host}"
    else:
        return f"{engine}://{host}"


def inspect_database(database_url: str):
    """Create an inspector for the given database URL."""
    engine = create_engine(database_url)
    return inspect(engine)


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


def get_sorted_tables(database_url: str) -> list[str]:
    """Get topologically sorted table names from a database."""
    inspector = inspect_database(database_url)
    graph = build_dependency_graph(inspector)
    return topological_sort(graph)


def write_config(config: dict, path) -> None:
    """Write config to file with blank lines between sections."""
    import yaml

    lines = []
    lines.append(f"version: {config['version']}")
    lines.append("")

    lines.append("database:")
    for key, value in config["database"].items():
        lines.append(f"  {key}: {value}")
    lines.append("")

    lines.append("schema:")
    for table, rows in config["schema"].items():
        lines.append(f"  {table}: {rows}")

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
