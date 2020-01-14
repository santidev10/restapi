S3_SEGMENT_EXPORT_KEY_PATTERN = "persistent-segments/{segment_type}/{segment_title}.csv"
S3_SEGMENT_BRAND_SAFETY_EXPORT_KEY_PATTERN = "persistent-segments/brand_safety/{segment_type}/{segment_title} - {datetime}.csv"
S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL = "https://s3.amazonaws.com/viewiq-prod/persistent-segments/thumbnails/cf-default.jpg"

class PersistentSegmentType:
    CHANNEL = "channel"
    VIDEO = "video"


class PersistentSegmentCategory:
    BLACKLIST = "blacklist"
    WHITELIST = "whitelist"
    TOPIC = "topic"
    APEX = "apex"


class PersistentSegmentTitles:
    CHANNELS_MASTER_BLACKLIST_SEGMENT_TITLE = "Channels Master Blacklist"
    CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE = "Channels Master Whitelist"

    VIDEOS_MASTER_BLACKLIST_SEGMENT_TITLE = "Videos Master Blacklist"
    VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE = "Videos Master Whitelist"

    CHANNELS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE = "Brand Suitability Master Blacklist Channels"
    CHANNELS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE = "Brand Suitability Master Whitelist Channels"

    VIDEOS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE = "Brand Suitability Master Blacklist Videos"
    VIDEOS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE = "Brand Suitability Master Whitelist Videos"

    ALL_MASTER_SEGMENT_TITLES = (
        CHANNELS_MASTER_BLACKLIST_SEGMENT_TITLE,
        CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
        VIDEOS_MASTER_BLACKLIST_SEGMENT_TITLE,
        VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE,
        CHANNELS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
        CHANNELS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE,
        VIDEOS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
        VIDEOS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE
    )

    MASTER_BLACKLIST_SEGMENT_TITLES = (
        CHANNELS_MASTER_BLACKLIST_SEGMENT_TITLE,
        VIDEOS_MASTER_BLACKLIST_SEGMENT_TITLE,
        CHANNELS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
        VIDEOS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE
    )

    MASTER_WHITELIST_SEGMENT_TITLES = (
        CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
        VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE,
        CHANNELS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE,
        VIDEOS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE
    )

    TITLES_MAP = (
        (PersistentSegmentType.CHANNEL, (
            (PersistentSegmentCategory.BLACKLIST, CHANNELS_MASTER_BLACKLIST_SEGMENT_TITLE),
            (PersistentSegmentCategory.WHITELIST, CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE),
        )),
        (PersistentSegmentType.VIDEO, (
            (PersistentSegmentCategory.BLACKLIST, VIDEOS_MASTER_BLACKLIST_SEGMENT_TITLE),
            (PersistentSegmentCategory.WHITELIST, VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE),
        )),
    )

    CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE = "Curated Channels Master Whitelist"

    NO_AUDIT_SEGMENTS = (
        CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
    )


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
    AUDITED_VIDEOS = "Audited_Videos"
    BAD_WORDS = "Bad_Words"
    CHANNEL_ID = "Channel_ID"
    CHANNEL_TITLE = "Channel_Title"
    OVERALL_SCORE = "Overall_Score"

    CHANNEL_BLACKLIST_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        LANGUAGE,
        SUBSCRIBERS,
        LIKES,
        DISLIKES,
        VIEWS,
        AUDITED_VIDEOS,
        OVERALL_SCORE
    )

    CHANNEL_WHITELIST_CSV_COLUMNS = (
        URL,
        TITLE,
        LANGUAGE,
        CATEGORY,
        SUBSCRIBERS,
        LIKES,
        DISLIKES,
        VIEWS,
        AUDITED_VIDEOS,
        OVERALL_SCORE
    )

    CHANNEL_TOPIC_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        SUBSCRIBERS,
        LIKES,
        DISLIKES,
        VIEWS,
        AUDITED_VIDEOS,
        BAD_WORDS,
    )

    VIDEO_BLACKLIST_CSV_COLUMNS = (
        URL,
        TITLE,
        LANGUAGE,
        CATEGORY,
        LIKES,
        DISLIKES,
        VIEWS,
        OVERALL_SCORE
    )

    VIDEO_WHITELIST_CSV_COLUMNS = (
        URL,
        TITLE,
        LANGUAGE,
        CATEGORY,
        LIKES,
        DISLIKES,
        VIEWS,
        OVERALL_SCORE
    )

    VIDEO_TOPIC_CSV_COLUMNS = (
        URL,
        TITLE,
        CATEGORY,
        LIKES,
        DISLIKES,
        VIEWS,
        BAD_WORDS,
        CHANNEL_ID,
        CHANNEL_TITLE
    )

    CHANNEL_CSV_COLUMNS_MAP_BY_CATEGORY = (
        (PersistentSegmentCategory.BLACKLIST, CHANNEL_BLACKLIST_CSV_COLUMNS),
        (PersistentSegmentCategory.WHITELIST, CHANNEL_WHITELIST_CSV_COLUMNS),
        (PersistentSegmentCategory.APEX, CHANNEL_WHITELIST_CSV_COLUMNS),
        (PersistentSegmentCategory.TOPIC, CHANNEL_TOPIC_CSV_COLUMNS),
    )

    VIDEO_CSV_COLUMNS_MAP_BY_CATEGORY = (
        (PersistentSegmentCategory.BLACKLIST, VIDEO_BLACKLIST_CSV_COLUMNS),
        (PersistentSegmentCategory.WHITELIST, VIDEO_WHITELIST_CSV_COLUMNS),
        (PersistentSegmentCategory.APEX, VIDEO_WHITELIST_CSV_COLUMNS),
        (PersistentSegmentCategory.TOPIC, VIDEO_TOPIC_CSV_COLUMNS),
    )

    CSV_COLUMNS_MAPS_BY_TYPE = (
        (PersistentSegmentType.CHANNEL, CHANNEL_CSV_COLUMNS_MAP_BY_CATEGORY),
        (PersistentSegmentType.VIDEO, VIDEO_CSV_COLUMNS_MAP_BY_CATEGORY),
    )

PERSISTENT_SEGMENT_CHANNEL_PREVIEW_FIELDS = (
    "channel_id",
    "brand_safety",
    "category",
    "country",
    "url",
    "language",
    "preferred",
    "subscribers",
    "thirty_days_subscribers",
    "daily_subscribers",
    "thirty_days_views",
    "weekly_views",
    "daily_views",
    "thumbnail_image_url",
    "title",
    "video_views",
    "videos",
    "views",
    "views_per_video",
    "verified",
)

PERSISTENT_SEGMENT_VIDEO_PREVIEW_FIELDS = (
    "video_id",
    "title",
    "views",
    "thirty_days_views",
    "weekly_views",
    "daily_views",
    "comments",
    "likes",
    "thirty_days_likes",
    "weekly_likes",
    "daily_likes",
    "dislikes",
    "thumbnail_image_url",
    "country",
    "category",
    "language",
    "is_flagged",
    "duration",
    "verified",
    "url",
    "license",
    "bad_statuses",
    "title_bad_words",
    "title_bad_statuses",
)

CATEGORY_THUMBNAIL_IMAGE_URLS = {
    "Film & Animation": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/FilmAnimation.jpg",
    "Autos & Vehicles": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/AutoVehicles.jpg",
    "Music": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Music.jpg",
    "Pets & Animals": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/PetsAnimals.png",
    "Sports": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Sports.jpg",
    "Travel & Events": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/TravelEvents.jpg",
    "Gaming": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Gaming.jpg",
    "People & Blogs": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/PeopleBlogs.jpg",
    "Comedy": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Comedy.jpg",
    "Entertainment": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Entertainment.jpg",
    "News & Politics": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/NewsPolitics.jpg",
    "Howto & Style": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/HowtoStyle.jpg",
    "Education": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Education.jpg",
    "Science & Technology": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/ScienceTechnology.jpg",
    "Nonprofits & Activism": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/NonprofitActivism.jpg",
    "Movies": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Movies.png",
    "Shows": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Shows.jpg",
    "Trailers": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/Trailers.jpg",
    "American Idol": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/AmericanIdol.jpg",
    "Game Of Thrones": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/GameOfThrones.jpg",
    "Stranger Things": "https://viewiq-prod.s3.amazonaws.com/persistent-segments/brand_safety/thumbnails/StrangerThings.jpg",
}

SHARED_SOURCE_FIELDS = ("main.id", "general_data.title",  "stats.views", "stats.last_30day_views",
                        "brand_safety.overall_score", "general_data.thumbnail_image_url", "brand_safety.language",
                        "ads_stats.ctr", "ads_stats.ctr_v", "ads_stats.video_view_rate", "ads_stats.average_cpm", "ads_stats.average_cpv")
CHANNEL_SOURCE_FIELDS = SHARED_SOURCE_FIELDS + ("general_data.top_category", "brand_safety.videos_scored", "stats.observed_videos_dislikes", "stats.last_30day_subscribers", "stats.subscribers", "stats.observed_videos_likes", "monetization.is_monetizable")
VIDEO_SOURCE_FIELDS = SHARED_SOURCE_FIELDS + ("general_data.language", "stats.likes", "stats.dislikes", "general_data.category")

YT_GENRE_CHANNELS = {
    "UC-9-kyTW8ZkZNDHQJ6FgpwQ",
    "UClgRkhTL3_hImCAmdLfDE4g",
    "UCOpNcN46UbXVtpKMrmU4Abg",
    "UCEgdi0XIXXZ-qJOFPf4JSKw",
    "UCYfdidRxbB8Qhf0Nx7ioOYw",
    "UCF0pVplsI8R5kcAqgtoRqoA",
    "UCQvWX73GQygcwXOTSf_VDVg",
    "UCl8dMTqDrJQ0c8y23UBu4kQ",
    "UC4R8DWoMoI7CAwX8_LjQHig",
    "UC1vGae2Q3oT5MkhhfW8lwjg",
    "UCAh9DbAZny_eoGFsYlH2JZw",
    "UCxAgnFbkxldX6YUEvdcNjnA",
    "UCR44SO_mdyRq-aTJHO5QxAw",
    "UCtxxJi5P0rk6rff3_dCfQVw",
    "UCmzy72gDEpfXoFV9Xdtd0DQ",
    "UC7DWJmY_p7qLzIy2-V77U5Q",
    "UCdxD5if2uHt2ZwIR6M1eBtg",
    "UCEOJ7BEM0V5dYWFNoj3qJZQ",
    "UCMlwQuZJJQppHO6-FkGgiqQ",
    "UCsVf5SnHAmJcZ0G7kpMcYzg",
    "UCBLGfSeyU52VOqZLhIm0xmg"
}
