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

CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY = "custom_segments/featured_images/{filename}.{extension}"
CUSTOM_SEGMENT_DEFAULT_IMAGE_URL = "https://viewiq-ui-assets.s3.amazonaws.com/common/default_audit_image.png"
