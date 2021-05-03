from es_components.constants import Sections

general_data_fields = [
    f"{Sections.GENERAL_DATA}.{field}" for field in ("title", "description", "lang_code", "video_tags", "tags")
]
CHANNEL_SOURCE = (
    Sections.MAIN, Sections.CUSTOM_PROPERTIES, Sections.TASK_US_DATA, Sections.BRAND_SAFETY, *general_data_fields,
)
TRANSCRIPT_SOURCE = (Sections.GENERAL_DATA, Sections.TEXT, Sections.VIDEO)
VIDEO_SOURCE = CHANNEL_SOURCE + (Sections.CHANNEL, Sections.CAPTIONS, Sections.CUSTOM_CAPTIONS)
