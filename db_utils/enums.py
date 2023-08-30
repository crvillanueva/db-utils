from enum import StrEnum


class FormatKeyWordOption(StrEnum):
    UPPER = "upper"
    LOWER = "lower"
    CAPITALIZE = "capitalize"


class OutputFormat(StrEnum):
    TSV = "tsv"
    TABLE = "table"
    JSON = "json"
