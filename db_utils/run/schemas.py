from typing import TypedDict


class QueryData(TypedDict):
    name: str
    query: str
    placeholders: list[str]
