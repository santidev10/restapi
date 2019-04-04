VIDEO = "video"
CHANNEL = "channel"

BRAND_SAFETY_PASS = "brand_safety_pass"
BRAND_SAFETY_FAIL = "brand_safety_fail"
BRAND_SAFETY = "brand_safety"
WHITELIST = "whitelist"
BLACKLIST = "blacklist"

BRAND_SAFETY_HITS = "brand_safety_hits"
WHITELIST_HITS = "whitelist_hits"
BLACKLIST_HITS = "blacklist_hits"

BRAND_SAFETY_PASS_VIDEOS = "brand_safety_pass_videos"
BRAND_SAFETY_PASS_CHANNELS = "brand_safety_pass_channels"
BRAND_SAFETY_FAIL_VIDEOS = "brand_safety_fail_videos"
BRAND_SAFETY_FAIL_CHANNELS = "brand_safety_fail_channels"

WHITELIST_VIDEOS = "whitelist_videos"
WHITELIST_CHANNELS = "whitelist_channels"
BLACKLIST_VIDEOS = "blacklist_videos"
BLACKLIST_CHANNELS = "blacklist_channels"

VIDEO_CATEGORIES = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "18": "Short Movies",
    "19": "Travel & Events",
    "20": "Gaming",
    "21": "Videoblogging",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism",
    "30": "Movies",
    "31": "Anime / Animation",
    "32": "Action / Adventure",
    "33": "Classics",
    "34": "Comedy",
    "37": "Family",
    "42": "Shorts",
    "43": "Shows",
    "44": "Trailers"
}

DISABLED = "Disabled"
UNKNOWN = "Unknown"
YOUTUBE = "youtube"
SDB = "sdb"

AUDIT_KEYWORD_MAPPING = {
    BRAND_SAFETY_FAIL: BRAND_SAFETY_HITS,
    WHITELIST: WHITELIST_HITS,
    BLACKLIST: BLACKLIST_HITS
}

BRAND_SAFETY_SCORE = "brand_safety_score"
