class PersistentSegmentExportColumn:
    URL = "URL"
    TITLE = "Title"
    CATEGORY = "Category"
    THUMBNAIL = "Thumbnail"
    LIKES = "Likes"
    DISLIKES = "Dislikes"
    VIEWS = "Views"
    AUDITED_VIDEOS = "Audited Videos"
    BAD_WORDS = "Bad Words"


PERSISTENT_SEGMENT_CSV_COLUMN_ORDER = (
    PersistentSegmentExportColumn.URL,
    PersistentSegmentExportColumn.TITLE,
    PersistentSegmentExportColumn.CATEGORY,
    PersistentSegmentExportColumn.THUMBNAIL,
    PersistentSegmentExportColumn.LIKES,
    PersistentSegmentExportColumn.DISLIKES,
    PersistentSegmentExportColumn.VIEWS,
)

PERSISTENT_SEGMENT_REPORT_HEADERS = {
    PersistentSegmentExportColumn.URL: "Url",
    PersistentSegmentExportColumn.TITLE: "Title",
    PersistentSegmentExportColumn.CATEGORY: "Category",
    PersistentSegmentExportColumn.THUMBNAIL: "Thumbnail",
    PersistentSegmentExportColumn.LIKES: "Likes",
    PersistentSegmentExportColumn.DISLIKES: "Dislikes",
    PersistentSegmentExportColumn.VIEWS: "Views",
    PersistentSegmentExportColumn.BAD_WORDS: "Bad Words",
    PersistentSegmentExportColumn.AUDITED_VIDEOS: "Audited Videos",
}

