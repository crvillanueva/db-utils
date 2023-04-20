import typer


def typer_error_msg_to_stdout(exc: Exception | str) -> None:
    if isinstance(exc, Exception):
        exc = str(exc)
    typer.secho(exc, fg=typer.colors.RED, err=True)
    raise typer.Exit(1)
