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

RESPONSE_OPTIONS = openapi.Schema(
    type=openapi.TYPE_ARRAY,
    items=openapi.Schema(
        type=openapi.TYPE_OBJECT,
        properties=dict(
            id=openapi.Schema(type=openapi.TYPE_STRING),
            name=openapi.Schema(type=openapi.TYPE_STRING),
        ),
    ),
)

CREATION_OPTIONS_SCHEMA = openapi.Schema(
    title="Creation options",
    description="Allowed options for account creations",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        name=openapi.Schema(type=openapi.TYPE_STRING),
        video_ad_format=openapi.Schema(
            type=openapi.TYPE_ARRAY,
            items=openapi.Schema(
                type=openapi.TYPE_OBJECT,
                properties=dict(
                    id=openapi.Schema(type=openapi.TYPE_STRING),
                    name=openapi.Schema(type=openapi.TYPE_STRING),
                    thumbnail=openapi.Schema(type=openapi.TYPE_STRING),
                ),
            )
        ),
        goal_type=RESPONSE_OPTIONS,
        type=RESPONSE_OPTIONS,
        bidding_type=RESPONSE_OPTIONS,
        delivery_method=RESPONSE_OPTIONS,
        video_networks=RESPONSE_OPTIONS,
        start=openapi.Schema(type=openapi.TYPE_STRING),
        end=openapi.Schema(type=openapi.TYPE_STRING),
        goal_units=openapi.Schema(type=openapi.TYPE_STRING),
        budget=openapi.Schema(type=openapi.TYPE_STRING),
        budget_type=openapi.Schema(type=openapi.TYPE_STRING),
        max_rate=openapi.Schema(type=openapi.TYPE_STRING),
        languages=RESPONSE_OPTIONS,
        devices=RESPONSE_OPTIONS,
        content_exclusions=RESPONSE_OPTIONS,
        frequency_capping=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties=dict(
                event_type=RESPONSE_OPTIONS,
                level=RESPONSE_OPTIONS,
                limit=openapi.Schema(type=openapi.TYPE_STRING),
                time_unit=RESPONSE_OPTIONS,
                __help=openapi.Schema(type=openapi.TYPE_STRING),
            ),
        ),
        location_rules=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties=dict(
                geo_target=openapi.Schema(type=openapi.TYPE_STRING),
                latitude=openapi.Schema(type=openapi.TYPE_STRING),
                longitude=openapi.Schema(type=openapi.TYPE_STRING),
                radius=openapi.Schema(type=openapi.TYPE_STRING),
                radius_units=RESPONSE_OPTIONS,
                __help=openapi.Schema(type=openapi.TYPE_STRING),
            ),
        ),
        ad_schedule_rules=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties=dict(
                day=RESPONSE_OPTIONS,
                from_hour=RESPONSE_OPTIONS,
                from_minute=RESPONSE_OPTIONS,
                to_hour=RESPONSE_OPTIONS,
                to_minute=RESPONSE_OPTIONS,
                __help=openapi.Schema(type=openapi.TYPE_STRING),
            ),
        ),
        video_url=openapi.Schema(type=openapi.TYPE_STRING),
        ct_overlay_text=openapi.Schema(type=openapi.TYPE_STRING),
        display_url=openapi.Schema(type=openapi.TYPE_STRING),
        final_url=openapi.Schema(type=openapi.TYPE_STRING),
        genders=RESPONSE_OPTIONS,
        parents=RESPONSE_OPTIONS,
        age_ranges=RESPONSE_OPTIONS,
    ),
)
