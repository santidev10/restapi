from drf_yasg import openapi

VIDEO_ITEM_SCHEMA = openapi.Schema(
    title="Youtube video",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        id=openapi.Schema(type=openapi.TYPE_STRING),
        title=openapi.Schema(type=openapi.TYPE_STRING),
        description=openapi.Schema(type=openapi.TYPE_STRING),
        thumbnail=openapi.Schema(type=openapi.TYPE_STRING),
        channel=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties=dict(
                id=openapi.Schema(type=openapi.TYPE_STRING),
                title=openapi.Schema(type=openapi.TYPE_STRING),
            ),
        ),
        duration=openapi.Schema(
            type=openapi.TYPE_NUMBER,
            format=openapi.FORMAT_DECIMAL
        )
    ),
)
VIDEO_RESPONSE_SCHEMA = openapi.Schema(
    title="Youtube video paginated response",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        next_page=openapi.Schema(type=openapi.TYPE_STRING),
        items_count=openapi.Schema(type=openapi.TYPE_INTEGER),
        items=openapi.Schema(
            title="Youtube video list",
            type=openapi.TYPE_ARRAY,
            items=VIDEO_ITEM_SCHEMA,
        ),
    ),
)

VIDEO_FORMAT_PARAMETER = openapi.Parameter(
    name="video_ad_format",
    required=False,
    in_=openapi.IN_QUERY,
    description="Video format for search",
    type=openapi.TYPE_STRING,
    enum=["BUMPER"]
)
