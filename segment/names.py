class PersistentSegmentExportColumn:
    URL = "URL"
    TITLE = "Title"
    CATEGORY = "Category"
    LANGUAGE = "Language"
    THUMBNAIL = "Thumbnail"
    LIKES = "Likes"
    DISLIKES = "Dislikes"
    VIEWS = "Views"
    SUBSCRIBERS = "Subscribers"
    AUDITED_VIDEOS = "Audited Videos"
    BAD_WORDS = "Bad Words"


PERSISTENT_SEGMENT_CHANNEL_CSV_COLUMNS = (
    PersistentSegmentExportColumn.URL,
    PersistentSegmentExportColumn.TITLE,
    PersistentSegmentExportColumn.CATEGORY,
    PersistentSegmentExportColumn.LANGUAGE,
    PersistentSegmentExportColumn.THUMBNAIL,
    PersistentSegmentExportColumn.SUBSCRIBERS,
    PersistentSegmentExportColumn.LIKES,
    PersistentSegmentExportColumn.DISLIKES,
    PersistentSegmentExportColumn.VIEWS,
    PersistentSegmentExportColumn.AUDITED_VIDEOS,
    PersistentSegmentExportColumn.BAD_WORDS,
)

PERSISTENT_SEGMENT_VIDEO_CSV_COLUMNS = (
    PersistentSegmentExportColumn.URL,
    PersistentSegmentExportColumn.TITLE,
    PersistentSegmentExportColumn.CATEGORY,
    PersistentSegmentExportColumn.LANGUAGE,
    PersistentSegmentExportColumn.THUMBNAIL,
    PersistentSegmentExportColumn.LIKES,
    PersistentSegmentExportColumn.DISLIKES,
    PersistentSegmentExportColumn.VIEWS,
    PersistentSegmentExportColumn.BAD_WORDS,
)

S3_SEGMENT_EXPORT_KEY_PATTERN = "persistent-segments/{segment_type}/{segment_title}.csv"
