"""Create the project's local database and .env without storing admin credentials.

Run from the repository root: python -m scripts.setup_local
"""

import argparse
import getpass
from pathlib import Path
import secrets

from dotenv import dotenv_values
import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-user", default="postgres")
    parser.add_argument("--port", type=int, default=5432)
    args = parser.parse_args()
    env_path = ROOT / ".env"
    existing = dotenv_values(env_path) if env_path.exists() else {}
    if existing.get("POSTGRES_HOST", "127.0.0.1") not in ("127.0.0.1", "localhost"):
        parser.exit(1, "Existing .env uses a remote database; it was not changed.\n")
    if existing and int(existing.get("POSTGRES_PORT", "5432")) != args.port:
        parser.exit(1, "Existing .env uses another port; pass the matching --port.\n")
    dbname = existing.get("POSTGRES_DB") or "p2p_tracker"
    app_user = existing.get("POSTGRES_USER") or "p2p_app"
    if app_user == args.admin_user:
        parser.exit(1, "Use a separate application user, not the admin account.\n")
    admin_password = getpass.getpass(f"Local PostgreSQL password for {args.admin_user}: ")
    try:
        with psycopg.connect(host="127.0.0.1", port=args.port, dbname="postgres",
                             user=args.admin_user, password=admin_password,
                             connect_timeout=5, autocommit=True) as admin:
            role = admin.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (app_user,)).fetchone()
            password = existing.get("POSTGRES_PASSWORD")
            if role:
                if not password or password == "change_me":
                    password = getpass.getpass(f"Existing application role {app_user} password: ")
            else:
                password = secrets.token_urlsafe(32)
                admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE").format(
                    sql.Identifier(app_user), sql.Literal(password)))
                print(f"Created application role {app_user} (password will be stored only in ignored .env).")
            database = admin.execute(
                "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=%s", (dbname,)).fetchone()
            if database:
                if database[0] != app_user:
                    raise ValueError("Existing database has another owner; no ownership/data was changed")
            else:
                admin.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(
                    sql.Identifier(dbname), sql.Identifier(app_user)))
                print(f"Created database {dbname}.")
        # Verify credentials and initialize as the app user, so it owns its tables.
        with psycopg.connect(host="127.0.0.1", port=args.port, dbname=dbname,
                             user=app_user, password=password, connect_timeout=5) as conn:
            conn.execute((ROOT / "database" / "schema.sql").read_text(encoding="utf-8"))
            print("Local connection and schema initialization: OK.")
        values = dict(dotenv_values(ROOT / ".env.example"))
        values.update(existing)
        values.update(POSTGRES_HOST="127.0.0.1", POSTGRES_PORT=str(args.port),
                      POSTGRES_DB=dbname, POSTGRES_USER=app_user, POSTGRES_PASSWORD=password,
                      TRACKER_HOST="127.0.0.1")
        # Quote values for python-dotenv; keep existing settings and a local backup.
        def quote(value):
            return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
        encoded = "\n".join(f"{key}={quote(value)}" for key, value in values.items()
                            if value is not None) + "\n"
        if env_path.exists():
            backup = ROOT / "data" / "local-setup" / ("env-backup-" + secrets.token_hex(4))
            backup.parent.mkdir(parents=True, exist_ok=True)
            backup.write_bytes(env_path.read_bytes())
        temporary = ROOT / "data" / "local-setup" / "env-new"
        temporary.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(encoded, encoding="utf-8")
        temporary.replace(env_path)
        print("Saved local .env. Admin password was not saved. Never commit .env or data/.")
        print("Next: python -m scripts.check_local")
    except (psycopg.Error, OSError, ValueError) as error:
        # Driver exceptions may contain connection details: report only the class.
        parser.exit(1, f"Setup failed ({type(error).__name__}); check credentials, role and database ownership.\n")


if __name__ == "__main__":
    main()
