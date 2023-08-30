# Database CLI Utilities

CLI tool for basic database operations with SQLAlchemy. It optionally requires [usql](https://github.com/xo/usql) for the `connect` command.

The tool searchs for a `DB_CONNECTION_URL` environmental variable.

# Usage

 Usage: dbu [OPTIONS] COMMAND [ARGS]...

 Database utilities with Python

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --env-file                    TEXT  Path to env-file. [default: .env]                                                                                      │
│ --db-url-key          -k      TEXT  Key name of the database URL in the env-file. [default: DB_CONNECTION_URL]                                             │
│ --install-completion                Install completion for the current shell.                                                                              │
│ --show-completion                   Show completion for the current shell, to copy it or customize the installation.                                       │
│ --help                              Show this message and exit.                                                                                            │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ autogen                   Autogenerate models/tables from database.                                                                                        │
│ connect                   Connect to interactive shell using usql.                                                                                         │
│ create-metatada           Create a file with the metadata of the database.                                                                                 │
│ format                    Format SQL query.                                                                                                                │
│ inspect                   Inspect database and its elements                                                                                                │
│ run                       Run sql script from file.                                                                                                        │
│ time-query                Time a query in seconds.                                                                                                         │
│ url                       Show the database URL and copy it to clipboard.                                                                                  │
│ viewgen                   Generate a view from a table that considers related tables given their foreign keys.                                             │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

# Examples

```bash
dbu inspect database tables
```

# Install autocompletion

dbu --install-completion
