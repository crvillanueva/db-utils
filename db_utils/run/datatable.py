import logging

import pyperclip
from rich.text import Text
from textual.app import App, ComposeResult, log
from textual.widgets import DataTable, Footer

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s:%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class TableApp(App):
    BINDINGS = [("c", "copy_cell_contents", "Copy"), ("b", "log_string", "Log")]

    def __init__(self, rows: list, **kwargs):
        self.rows = rows
        super().__init__(**kwargs)

    def compose(self) -> ComposeResult:
        yield DataTable()
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "cell"
        table.zebra_stripes = True
        table.add_columns(*self.rows[0].keys())
        row_tuples = [tuple(row.values()) for row in self.rows]
        for row in row_tuples:
            styled_row = [
                Text(str(cell), style="italic #03AC13", justify="right") for cell in row
            ]
            table.add_row(*styled_row)

    def action_log_string(self):
        log("Test")

    def action_copy_cell_contents(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_coordinate
        row = table.get_row_at(table.cursor_coordinate.row)
        cell = row[table.cursor_coordinate.column]
        pyperclip.copy(str(cell))


if __name__ == "__main__":
    TableApp(rows=[{"A": 1, "B": 2, "C": 3}]).run()
