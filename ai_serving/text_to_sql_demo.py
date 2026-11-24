"""Ask a Gold warehouse a question using an offline fixture or a local Ollama model."""

import argparse
import json
import os
import threading
import unicodedata
from pathlib import Path
from urllib.request import Request, urlopen

import duckdb
import sqlglot
import yaml
from dotenv import load_dotenv
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

ROOT = Path(__file__).resolve().parents[1]
GOLD = {"dim_customers", "dim_products", "fct_orders", "mart_daily_revenue"}
EXAMPLES = {
    "Doanh thu thuần theo hạng thành viên là bao nhiêu?": (
        "SELECT membership_tier, SUM(net_revenue) AS net_revenue "
        "FROM mart_daily_revenue GROUP BY membership_tier ORDER BY membership_tier"
    ),
    "GMV theo ngày là bao nhiêu?": (
        "SELECT order_date, SUM(gmv) AS gmv FROM mart_daily_revenue "
        "GROUP BY order_date ORDER BY order_date"
    ),
    "Có bao nhiêu đơn hàng hợp lệ?": "SELECT COUNT(order_id) AS total_orders FROM fct_orders",
}
# A deliberately small analytical SQL dialect. Unrecognised syntax fails closed.
ALLOWED_NODES = {
    "select",
    "from",
    "from_",
    "table",
    "column",
    "identifier",
    "alias",
    "tablealias",
    "where",
    "and",
    "or",
    "not",
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "between",
    "literal",
    "null",
    "boolean",
    "is",
    "group",
    "order",
    "ordered",
    "having",
    "limit",
    "sum",
    "avg",
    "min",
    "max",
    "count",
    "distinct",
    "add",
    "sub",
    "mul",
    "div",
    "paren",
    "join",
    "with",
    "with_",
    "cte",
    "subquery",
    "case",
    "if",
    "coalesce",
    "neg",
}


def metadata(path: Path) -> dict:
    """Read the same business definitions from Gold schema.yml or a compiled manifest."""
    content = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        nodes = json.loads(content)["nodes"].values()
        models = [
            n
            for n in nodes
            if n.get("resource_type") == "model"
            and n.get("package_name") == "ecommerce"
            and n["name"] in GOLD
        ]
    else:
        models = yaml.safe_load(content)["models"]
    context = {}
    for model in models:
        name = model["name"]
        if name not in GOLD:
            continue
        columns = model["columns"]
        columns = columns.values() if isinstance(columns, dict) else columns
        context[name] = {
            "description": model["description"],
            "columns": {c["name"]: c["description"] for c in columns},
        }
    if set(context) != GOLD or any(
        not all(m["columns"].values()) for m in context.values()
    ):
        raise ValueError("Gold metadata is incomplete")
    return context


def normalize(question: str) -> str:
    return " ".join(
        unicodedata.normalize("NFC", question).casefold().strip(" ?!.").split()
    )


def generate_sql(question: str, context: dict, model: str | None) -> str:
    if model is None:
        for example, sql in EXAMPLES.items():
            if normalize(question) == normalize(example):
                return sql
        raise ValueError("Offline examples:\n" + "\n".join(EXAMPLES))
    instructions = (
        "Generate one read-only DuckDB SELECT answering the question using only the supplied Gold "
        "relations and business definitions. List columns explicitly. Use aggregates, joins, "
        "comparisons and CTEs as needed. Return JSON containing a single string field sql. "
        "For an unsupported question return an empty sql. Amounts are VND and dates are UTC.\n"
        + json.dumps(context, ensure_ascii=False)
    )
    body = {
        "model": model,
        "stream": False,
        "format": {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"],
        },
        "options": {"temperature": 0, "num_predict": 512},
        "messages": [
            {"role": "system", "content": instructions},
            {"role": "user", "content": question},
        ],
    }
    request = Request(
        "http://127.0.0.1:11434/api/chat",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=120) as response:
        content = json.load(response)["message"]["content"]
    return json.loads(content)["sql"]


def validated_sql(sql: str) -> str:
    if not isinstance(sql, str) or not sql.strip() or len(sql) > 16_000:
        raise ValueError(
            "Expected a non-empty analytical SELECT of at most 16000 characters"
        )
    statements = sqlglot.parse(sql, read="duckdb")
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise ValueError("Exactly one SELECT is required")
    tree = statements[0]
    for node in tree.walk():
        if (
            isinstance(node, exp.Star)
            and isinstance(node.parent, exp.Count)
            and not any(node.args.values())
        ):
            continue
        if node.key not in ALLOWED_NODES:
            raise ValueError(f"Unsupported SQL operation: {node.key}")
        if isinstance(node, exp.With) and node.args.get("recursive"):
            raise ValueError("Recursive queries are unsupported")
        if isinstance(node, exp.Table) and (
            node.db or node.catalog or not isinstance(node.this, exp.Identifier)
        ):
            raise ValueError("Use an unqualified Gold table name")
    relations = set()
    for scope in traverse_scope(tree):
        for _, source in scope.selected_sources.values():
            if isinstance(source, exp.Table):
                relations.add(source.name.lower())
    if not relations or not relations <= GOLD:
        raise ValueError("Query must read only the four Gold relations")
    return tree.sql(dialect="duckdb", comments=False)


def query(database: Path, sql: str) -> dict:
    statement = validated_sql(sql)
    with duckdb.connect(
        str(database),
        read_only=True,
        config={
            "enable_external_access": "false",
            "threads": 2,
            "memory_limit": "512MB",
            "allow_community_extensions": "false",
        },
    ) as con:
        con.execute("set TimeZone='UTC'")
        con.execute("set lock_configuration=true")
        timeout = threading.Timer(15, con.interrupt)
        timeout.start()
        try:
            result = con.execute(statement)
            columns = [c[0] for c in result.description]
            rows = result.fetchmany(101)
        finally:
            timeout.cancel()
            timeout.join()
    return {
        "sql": statement,
        "columns": columns,
        "rows": rows[:100],
        "truncated": len(rows) > 100,
    }


def main() -> None:
    load_dotenv(Path.cwd() / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("question", nargs="?")
    parser.add_argument("--examples", action="store_true")
    parser.add_argument(
        "--model",
        help="Installed local Ollama model. Default: deterministic offline mock.",
    )
    parser.add_argument(
        "--metadata", type=Path, default=ROOT / "dbt_project/models/gold/schema.yml"
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(os.getenv("DUCKDB_PATH", "data/ecommerce.duckdb")),
    )
    args = parser.parse_args()
    if args.examples:
        print("\n".join(EXAMPLES))
        return
    if not args.question:
        parser.error("Provide a question or use --examples")
    try:
        sql = generate_sql(args.question, metadata(args.metadata), args.model)
        answer = query(args.database, sql)
    except (
        ValueError,
        OSError,
        KeyError,
        sqlglot.errors.SqlglotError,
        duckdb.Error,
    ) as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(answer, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
