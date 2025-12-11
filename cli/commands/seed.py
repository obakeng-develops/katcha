import random
from pathlib import Path

import typer
import yaml
from faker import Faker
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    Time,
)

from cli.commands.schema import build_database_url, build_dependency_graph, topological_sort

app = typer.Typer()
fake = Faker()

def get_faker_value(column_name: str, column_type, nullable: bool):
    """Generate a fake value based on column name and type."""
    if nullable and random.random() < 0.1:
        return None

    name_lower = column_name.lower()

    # Name-based heuristics (more specific matches first)
    if "email" in name_lower:
        return fake.email()
    if "phone" in name_lower or "mobile" in name_lower or "tel" in name_lower:
        return fake.phone_number()[:20]
    if name_lower in ("first_name", "firstname", "fname"):
        return fake.first_name()
    if name_lower in ("last_name", "lastname", "lname", "surname"):
        return fake.last_name()
    if name_lower in ("name", "full_name", "fullname", "username"):
        return fake.name()
    if "address" in name_lower:
        return fake.address().replace("\n", ", ")
    if "city" in name_lower:
        return fake.city()
    if "state" in name_lower or "province" in name_lower:
        return fake.state()
    if "country" in name_lower:
        return fake.country()
    if "zip" in name_lower or "postal" in name_lower:
        return fake.postcode()
    if "url" in name_lower or "website" in name_lower or "link" in name_lower:
        return fake.url()
    if "ip" in name_lower and "address" in name_lower:
        return fake.ipv4()
    if name_lower == "ip" or name_lower.endswith("_ip"):
        return fake.ipv4()
    if "description" in name_lower or "desc" in name_lower:
        return fake.paragraph()
    if "title" in name_lower:
        return fake.sentence(nb_words=4).rstrip(".")
    if "company" in name_lower or "organization" in name_lower:
        return fake.company()
    if "uuid" in name_lower or "guid" in name_lower:
        return str(fake.uuid4())
    if "password" in name_lower or "hash" in name_lower:
        return fake.sha256()
    if "token" in name_lower or "secret" in name_lower:
        return fake.sha1()
    if "slug" in name_lower:
        return fake.slug()
    if "color" in name_lower or "colour" in name_lower:
        return fake.hex_color()
    if "domain" in name_lower:
        return fake.domain_name()
    if "created_at" in name_lower or "updated_at" in name_lower or "last_login" in name_lower:
        return fake.date_time()
    if name_lower.endswith("_on") or name_lower.endswith("_at"):
        return fake.date_time()
    if "score" in name_lower or "rating" in name_lower:
        return round(fake.pyfloat(min_value=0, max_value=100), 2)
    if "discount" in name_lower:
        return round(random.uniform(0, 0.5), 2)  # 0-50% discount
    if "quantity" in name_lower:
        return fake.random_int(min=1, max=100)
    if "price" in name_lower or "cost" in name_lower or "amount" in name_lower:
        return round(fake.pyfloat(min_value=1, max_value=1000), 2)
    if "count" in name_lower:
        return fake.random_int(min=0, max=1000)
    if "version" in name_lower:
        return fake.random_int(min=1, max=10)
    if "serial" in name_lower:
        return fake.hexify(text="^^^^^^^^^^^^^^^^")
    if "subject" in name_lower or "issuer" in name_lower:
        return fake.company()
    if "algorithm" in name_lower:
        return random.choice(["RSA", "ECDSA", "SHA256", "SHA384", "SHA512"])
    if "public_key" in name_lower or "key" in name_lower:
        return fake.sha256()
    if "status" in name_lower:
        return random.choice(["active", "inactive", "pending", "completed"])
    if "role" in name_lower:
        return random.choice(["admin", "user", "guest", "moderator"])
    if "type" in name_lower:
        return fake.word()
    if name_lower.endswith("id") and "uuid" not in name_lower:
        # ID columns that aren't UUIDs - generate short alphanumeric codes
        return fake.bothify(text="???##").upper()

    # Type-based fallbacks - check both class and string representation
    type_class = type(column_type)
    type_str = str(column_type).upper()

    # UUID type detection (PostgreSQL UUID, or CHAR(36)/VARCHAR(36) for UUID storage)
    if "UUID" in type_str:
        return str(fake.uuid4())
    if type_class in (Integer, SmallInteger, BigInteger) or "INT" in type_str:
        return fake.random_int(min=1, max=10000)
    if type_class in (Float, Numeric) or "FLOAT" in type_str or "NUMERIC" in type_str or "DECIMAL" in type_str or "REAL" in type_str:
        return round(fake.pyfloat(min_value=0, max_value=10000), 2)
    if type_class == Boolean or "BOOL" in type_str:
        return fake.boolean()
    if type_class == Date or type_str == "DATE":
        return fake.date_object()
    if type_class == DateTime or "DATETIME" in type_str or "TIMESTAMP" in type_str:
        return fake.date_time()
    if type_class == Time or type_str == "TIME":
        return fake.time_object()
    if "JSON" in type_str:
        return None  # JSON columns often need specific structure
    if type_class == Text or "TEXT" in type_str:
        return fake.paragraph()
    if type_class == String or "VARCHAR" in type_str or "CHAR" in type_str:
        length = getattr(column_type, "length", None) or 255
        # CHAR(36) or VARCHAR(36) is commonly used for UUID storage
        if length == 36:
            return str(fake.uuid4())
        generated = fake.text(max_nb_chars=min(length, 200))
        return generated[:length]

    # Default fallback
    return fake.word()

def get_table_columns(inspector, table_name: str) -> list[dict]:
    """Get column information for a table."""
    return inspector.get_columns(table_name)

def get_foreign_keys(inspector, table_name: str) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    """Get foreign key mappings: local_column -> (referred_table, referred_column).

    Returns:
        (regular_fks, self_referential_fks) - two dicts separating self-refs
    """
    fk_map = {}
    self_ref_map = {}
    for fk in inspector.get_foreign_keys(table_name):
        for local_col, referred_col in zip(fk["constrained_columns"], fk["referred_columns"]):
            if fk["referred_table"] == table_name:
                self_ref_map[local_col] = (fk["referred_table"], referred_col)
            else:
                fk_map[local_col] = (fk["referred_table"], referred_col)
    return fk_map, self_ref_map

def get_primary_keys(inspector, table_name: str) -> set[str]:
    """Get primary key column names for a table."""
    pk = inspector.get_pk_constraint(table_name)
    return set(pk.get("constrained_columns", []))

def get_unique_columns(inspector, table_name: str) -> set[str]:
    """Get columns that have unique constraints."""
    unique_cols = set()
    for idx in inspector.get_indexes(table_name):
        if idx.get("unique"):
            unique_cols.update(idx.get("column_names", []))
    return unique_cols

@app.command()
def seed(
    config_file: Path = typer.Option(
        Path("katcha.yml"),
        "--config", "-c",
        help="Path to katcha.yml config file"
    ),
):
    """Seed the database with fake data based on katcha.yml configuration."""
    if not config_file.exists():
        typer.echo(f"Error: Config file not found: {config_file}", err=True)
        typer.echo("Run 'katcha init <database_url>' first.", err=True)
        raise typer.Exit(1)

    with open(config_file) as f:
        config = yaml.safe_load(f)

    if "database" not in config:
        typer.echo("Error: No database configuration found.", err=True)
        raise typer.Exit(1)

    database_url = build_database_url(config["database"])
    engine = create_engine(database_url)
    inspector = inspect(engine)

    schema = config.get("schema", {})
    if not schema:
        typer.echo("Error: No schema defined in config.", err=True)
        raise typer.Exit(1)

    # Build dependency graph and sort tables
    graph = build_dependency_graph(inspector)
    sorted_tables = topological_sort(graph)

    # Filter to only tables in config that exist in DB
    db_tables = set(inspector.get_table_names())
    tables_to_seed = [t for t in sorted_tables if t in schema and t in db_tables]

    # Track inserted PKs for foreign key references
    inserted_pks: dict[str, list] = {}

    typer.echo(f"Seeding {len(tables_to_seed)} tables...")

    with engine.connect() as conn:
        for table_name in tables_to_seed:
            row_count = schema[table_name]
            if row_count <= 0:
                continue

            columns = get_table_columns(inspector, table_name)
            foreign_keys, self_ref_fks = get_foreign_keys(inspector, table_name)
            primary_keys = get_primary_keys(inspector, table_name)
            unique_columns = get_unique_columns(inspector, table_name)

            # Check if this is a composite PK (junction table)
            is_composite_pk = len(primary_keys) > 1

            # Filter out auto-increment PKs (only single INTEGER PKs)
            insertable_columns = []
            auto_pk_col = None
            for col in columns:
                is_pk = col["name"] in primary_keys
                is_int_type = isinstance(col["type"], (Integer, SmallInteger, BigInteger))
                # Only skip single INTEGER PRIMARY KEY (auto-increment)
                if is_pk and is_int_type and not is_composite_pk:
                    auto_pk_col = col["name"]
                    continue
                insertable_columns.append(col)

            inserted_pks[table_name] = []

            # Track used values for unique columns
            used_unique_values: dict[str, set] = {col: set() for col in unique_columns}

            # Track used composite PK combinations
            used_pk_combinations: set[tuple] = set()

            # For unique FK columns, we need available (unused) PKs from referenced table
            available_fk_pks: dict[str, list] = {}
            for col_name in foreign_keys:
                if col_name in unique_columns:
                    ref_table, _ = foreign_keys[col_name]
                    if ref_table in inserted_pks:
                        available_fk_pks[col_name] = list(inserted_pks[ref_table])

            for _ in range(row_count):
                row_data = {}

                # For composite PK tables, try multiple times to find unique combination
                max_attempts = 100 if is_composite_pk else 1

                for attempt in range(max_attempts):
                    row_data = {}

                    for col in insertable_columns:
                        col_name = col["name"]
                        col_type = col["type"]
                        # PK columns are never nullable
                        is_pk_col = col_name in primary_keys
                        nullable = col.get("nullable", True) and not is_pk_col

                        # Handle self-referential FKs - set to NULL first, update later
                        if col_name in self_ref_fks:
                            row_data[col_name] = None
                        # Handle regular foreign keys
                        elif col_name in foreign_keys:
                            ref_table, ref_col = foreign_keys[col_name]

                            # Check if this FK has a unique constraint (but not part of composite PK)
                            if col_name in unique_columns and col_name not in primary_keys:
                                if col_name in available_fk_pks and available_fk_pks[col_name]:
                                    row_data[col_name] = available_fk_pks[col_name].pop(0)
                                elif nullable:
                                    row_data[col_name] = None
                                else:
                                    break
                            else:
                                # Non-unique FK or part of composite PK
                                if ref_table in inserted_pks and inserted_pks[ref_table]:
                                    row_data[col_name] = random.choice(inserted_pks[ref_table])
                                elif nullable:
                                    row_data[col_name] = None
                                else:
                                    row_data[col_name] = 1  # Fallback
                        elif col_name in unique_columns or is_pk_col:
                            # Generate unique value for unique/PK columns
                            track_key = col_name
                            if track_key not in used_unique_values:
                                used_unique_values[track_key] = set()
                            for _ in range(100):
                                value = get_faker_value(col_name, col_type, nullable=False)
                                if value not in used_unique_values[track_key]:
                                    used_unique_values[track_key].add(value)
                                    row_data[col_name] = value
                                    break
                            else:
                                base = get_faker_value(col_name, col_type, nullable=False)
                                value = f"{base}_{len(used_unique_values[track_key])}"
                                used_unique_values[track_key].add(value)
                                row_data[col_name] = value
                        else:
                            row_data[col_name] = get_faker_value(col_name, col_type, nullable)

                    # For composite PKs, check if combination is unique
                    if is_composite_pk and row_data:
                        pk_combo = tuple(row_data.get(pk) for pk in sorted(primary_keys))
                        if pk_combo not in used_pk_combinations:
                            used_pk_combinations.add(pk_combo)
                            break  # Found unique combination
                        else:
                            row_data = {}  # Reset and try again
                    else:
                        break  # Not composite PK, no need to retry

                # Skip if row is incomplete (e.g., ran out of unique FKs)
                if not row_data or len(row_data) < len(insertable_columns):
                    continue

                # Build and execute insert (quote table name for spaces)
                quoted_table = f'"{table_name}"' if " " in table_name else table_name
                col_names = ", ".join(f'"{k}"' for k in row_data.keys())
                placeholders = ", ".join(f":{k}" for k in row_data.keys())
                sql = text(f"INSERT INTO {quoted_table} ({col_names}) VALUES ({placeholders})")

                result = conn.execute(sql, row_data)

                # Track inserted PK for foreign key references
                # For single INTEGER PKs, use lastrowid; for others, use the value from row_data
                if len(primary_keys) == 1:
                    pk_col = list(primary_keys)[0]
                    if pk_col in row_data:
                        # Non-autoincrement PK (TEXT or composite)
                        inserted_pks[table_name].append(row_data[pk_col])
                    elif result.lastrowid:
                        # Auto-increment INTEGER PK
                        inserted_pks[table_name].append(result.lastrowid)
                else:
                    # Composite PK - just track that we inserted a row
                    inserted_pks[table_name].append(True)

            conn.commit()

            # Second pass: update self-referential FKs
            if self_ref_fks and inserted_pks[table_name]:
                pks = inserted_pks[table_name]
                # Determine the PK column name for the WHERE clause
                if auto_pk_col:
                    pk_col_name = auto_pk_col
                elif len(primary_keys) == 1:
                    pk_col_name = list(primary_keys)[0]
                else:
                    pk_col_name = None  # Can't easily update composite PKs

                if pk_col_name:
                    for self_ref_col, (_, ref_col) in self_ref_fks.items():
                        # Update ~50% of rows to reference other rows
                        rows_to_update = random.sample(pks, k=min(len(pks) // 2, len(pks)))
                        for pk_value in rows_to_update:
                            # Pick a different row to reference (avoid self-reference loops)
                            other_pks = [p for p in pks if p != pk_value]
                            if other_pks:
                                ref_value = random.choice(other_pks)
                                update_sql = text(
                                    f'UPDATE {quoted_table} SET "{self_ref_col}" = :ref_val '
                                    f'WHERE "{pk_col_name}" = :pk_val'
                                )
                                conn.execute(update_sql, {"ref_val": ref_value, "pk_val": pk_value})
                    conn.commit()

            actual_rows = len(inserted_pks[table_name])
            if actual_rows < row_count:
                typer.echo(f"  {table_name}: {actual_rows}/{row_count} rows (limited by constraints)")
            else:
                typer.echo(f"  {table_name}: {actual_rows} rows")

    typer.echo("Seeding complete!")

if __name__ == "__main__":
    app()
