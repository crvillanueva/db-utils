import logging

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

    def __init__(self, rows, **kwargs):
        self.rows = rows
        super().__init__(**kwargs)

    def compose(self) -> ComposeResult:
        yield DataTable()
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "cell"
        table.zebra_stripes = True
        self.log(self.rows)
        table.add_columns(*self.rows[0].keys())
        row_tuples = [tuple(row.values()) for row in self.rows]
        for row in row_tuples:
            styled_row = [
                Text(str(cell), style="italic #03AC13", justify="right") for cell in row
            ]
            table.add_row(*styled_row)

    # def on_key(self, event: events.Key) -> None:
    #     key = event.key
    #     if key == "c":
    #         self.action_copy_cell_contents()

    def action_log_string(self):
        log("Test")

    def action_copy_cell_contents(self) -> None:
        table = self.query_one(DataTable)
        self.log(table.cursor_coordinate)
        # pyperclip.copy(table.get_cell(*table.cursor_coordinate))


if __name__ == "__main__":
    TableApp(rows=[{"A": 1, "B": 2, "C": 3}]).run()
