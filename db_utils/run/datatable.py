import logging
from typing import Any

import pyperclip
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s:%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class TableApp(App):
    BINDINGS = [
        ("c", "copy_cell_contents", "Copy"),
        ("b", "log_string", "Log"),
        ("ctrl+d", "down_ten", "Down 10"),
        ("ctrl+u", "up_ten", "Up 10"),
        # vim style bindings
        Binding("j", "down", "", show=False, priority=True),
        Binding("k", "up", "", show=False, priority=True),
        Binding("h", "left", "", show=False, priority=True),
        Binding("l", "right", "", show=False, priority=True),
    ]

    def __init__(self, rows: list, **kwargs):
        self.rows: list[dict[str, Any]] = rows
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

    def action_copy_cell_contents(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_coordinate
        row = table.get_row_at(table.cursor_coordinate.row)
        cell = row[table.cursor_coordinate.column]
        pyperclip.copy(str(cell))

    def action_down_ten(self):
        table = self.query_one(DataTable)
        table.move_cursor(row=table.cursor_coordinate.row + 10)

    def action_up_ten(self):
        table = self.query_one(DataTable)
        table.move_cursor(row=table.cursor_coordinate.row - 10)

    def action_down(self):
        table = self.query_one(DataTable)
        table.move_cursor(row=table.cursor_coordinate.row + 1)

    def action_up(self):
        table = self.query_one(DataTable)
        table.move_cursor(row=table.cursor_coordinate.row - 1)

    def action_left(self):
        table = self.query_one(DataTable)
        table.move_cursor(column=table.cursor_coordinate.column - 1)

    def action_right(self):
        table = self.query_one(DataTable)
        table.move_cursor(column=table.cursor_coordinate.column + 1)


if __name__ == "__main__":
    pass
