import enum

VETTED_MAPPING = {
    0: "Skipped: Unavailable",
    1: "Skipped: Region",
    2: "Not Suitable",
    3: "Suitable",
    4: None # Item has not been vetted
}


class SourceListType(enum.Enum):
    INCLUSION = 0
    EXCLUSION = 1