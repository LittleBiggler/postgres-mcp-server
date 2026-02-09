from typing import List, Dict, Any
import os
import psycopg2
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initializes your MCP server instance. It's used to register your tools.
mcp = FastMCP("postgres-server")

# Database connection configuration from environment variables
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "practice_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# TODO: Implement a second MCP tool called `execute_sql`
# This function should:
#  - Take a SQL query as input (string)
#  - Run the query against the Postgres database
#  - Return the rows as a list of dictionaries (column_name → value)
# Hint: Use the same psycopg2 connection pattern shown in `get_schema`.
@mcp.tool()
async def execute_sql(sql: str) -> List[Dict]:
    """Execute a SQL query and return the rows as a list of dictionaries (column_name → value)."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows

# TODO: Implement a third MCP tool called `list_tables`
# This function should:
#  - Take no inputs
#  - Return the list of table names available in the current database
# Hint: Query `information_schema.tables` and filter for `table_schema = 'public'`.

@mcp.tool()
async def list_tables() -> Dict[str, List[str]]:
    """Return the list of table names available in the current database."""
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema in ('public', 'marts')
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = [r[0] for r in cur.fetchall()]
    return {"tables": rows}  # -- same return
    

@mcp.tool()
async def get_schema(table: str) -> List[Dict]:
    """Return column names and types for a given table."""
    sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (table,))
            rows = [{"column": r[0], "type": r[1]} for r in cur.fetchall()]
    return rows

@mcp.tool()
async def sanity_checks(
    active_status: str = "active",
    expired_status: str = "expired",
    sample_n: int = 20
) -> List[Dict[str, Any]]:
    """
    Basic data sanity checks:
      1) Duplicate user_id in public.users
      2) Active subs with end_date, and expired subs with no end_date
      3) Active users with no session data

    Returns a list of issue objects: {check, n, sample}
    """
    # cap sample size
    try:
        sample_n = _cap_limit(sample_n, default=20, max_value=200)
    except NameError:
        sample_n = 20 if (sample_n is None or sample_n < 1) else min(sample_n, 200)

    issues: List[Dict[str, Any]] = []

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            # -------------------------
            # 1) Duplicate user_ids
            # -------------------------
            cur.execute("""
                SELECT COUNT(*)
                FROM (
                  SELECT user_id
                  FROM public.users
                  GROUP BY user_id
                  HAVING COUNT(*) > 1
                ) t;
            """)
            dup_n = cur.fetchone()[0]

            cur.execute("""
                SELECT user_id, COUNT(*) AS n_rows
                FROM public.users
                GROUP BY user_id
                HAVING COUNT(*) > 1
                ORDER BY n_rows DESC, user_id
                LIMIT %s;
            """, (sample_n,))
            dup_sample = [{"user_id": r[0], "n_rows": r[1]} for r in cur.fetchall()]

            issues.append({
                "check": "duplicate_user_ids",
                "n": int(dup_n),
                "sample": dup_sample
            })

            # -----------------------------------------
            # 2a) Active subscriptions with an end_date
            # -----------------------------------------
            cur.execute("""
                SELECT COUNT(*)
                FROM public.subscriptions
                WHERE status = %s
                  AND end_date IS NOT NULL;
            """, (active_status,))
            active_end_n = cur.fetchone()[0]

            cur.execute("""
                SELECT user_id, subscription_id, plan, status, start_date, end_date
                FROM public.subscriptions
                WHERE status = %s
                  AND end_date IS NOT NULL
                ORDER BY end_date DESC NULLS LAST
                LIMIT %s;
            """, (active_status, sample_n))
            active_end_sample = [
                {
                    "user_id": r[0],
                    "subscription_id": r[1],
                    "plan": r[2],
                    "status": r[3],
                    "start_date": r[4],
                    "end_date": r[5],
                }
                for r in cur.fetchall()
            ]

            issues.append({
                "check": "active_with_end_date",
                "n": int(active_end_n),
                "sample": active_end_sample
            })

            # -----------------------------------------
            # 2b) Expired subscriptions with NO end_date
            # -----------------------------------------
            cur.execute("""
                SELECT COUNT(*)
                FROM public.subscriptions
                WHERE status = %s
                  AND end_date IS NULL;
            """, (expired_status,))
            expired_no_end_n = cur.fetchone()[0]

            cur.execute("""
                SELECT user_id, subscription_id, plan, status, start_date, end_date
                FROM public.subscriptions
                WHERE status = %s
                  AND end_date IS NULL
                ORDER BY start_date DESC NULLS LAST
                LIMIT %s;
            """, (expired_status, sample_n))
            expired_no_end_sample = [
                {
                    "user_id": r[0],
                    "subscription_id": r[1],
                    "plan": r[2],
                    "status": r[3],
                    "start_date": r[4],
                    "end_date": r[5],
                }
                for r in cur.fetchall()
            ]

            issues.append({
                "check": "expired_no_end_date",
                "n": int(expired_no_end_n),
                "sample": expired_no_end_sample
            })

            # -----------------------------------------
            # 3) Active users with no sessions
            # -----------------------------------------
            cur.execute("""
                SELECT COUNT(*)
                FROM (
                  SELECT s.user_id
                  FROM public.subscriptions s
                  LEFT JOIN public.sessions se
                    ON se.user_id = s.user_id
                  WHERE s.status = %s
                  GROUP BY s.user_id
                  HAVING COUNT(se.session_id) = 0
                ) t;
            """, (active_status,))
            no_sessions_n = cur.fetchone()[0]

            cur.execute("""
                SELECT s.user_id
                FROM public.subscriptions s
                LEFT JOIN public.sessions se
                  ON se.user_id = s.user_id
                WHERE s.status = %s
                GROUP BY s.user_id
                HAVING COUNT(se.session_id) = 0
                ORDER BY s.user_id
                LIMIT %s;
            """, (active_status, sample_n))
            no_sessions_sample = [{"user_id": r[0]} for r in cur.fetchall()]

            issues.append({
                "check": "active_no_sessions",
                "n": int(no_sessions_n),
                "sample": no_sessions_sample
            })

    return issues


def main():
    # Run MCP server using stdio transport for AI assistant integration
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
