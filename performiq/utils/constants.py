import enum


class CSVFieldTypeEnum(enum.Enum):
    URL = "URL"
    IMPRESSIONS = "Impressions"
    VIEWS = "Views"
    COST = "Cost"
    AVERAGE_CPV = "Avg CPV"
    AVERAGE_CPM = "Avg CPM"
    VIEW_RATE = "View Rate"
    VIDEO_PLAYED_TO_100_RATE = "Video Played to 100%"
    CTR = "CTR"
