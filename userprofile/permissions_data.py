# tmp file

class Permissions:
    """
    purpose: fill db with permissions sets

    for k,v in dict(PERMISSION_SETS).items():
        PermissionSet.objects.create(permission_set=k, permissions_values=list(v))


    """
    PERMISSION_SETS = (
        ('Trendings',                       ("view_highlights",)),
        ('Discovery',                       ("channel_list",
                                             "channel_filter",
                                             "channel_details",
                                             "video_list",
                                             "video_filter",
                                             "video_details",
                                             "keyword_list",
                                             "keyword_details",
                                             "keyword_filter",)),
        ('Segments',                        ("segment_video_private",
                                             "segment_channel_private",
                                             "segment_keyword_private",)),
        ('Segments - pre-baked segments',   ("segment_video_all",
                                             "segment_channel_all",
                                             "segment_keyword_all",
                                             "view_pre_baked_segments",)),
        ('Media buying',                    ("view_media_buying",
                                             "settings_my_aw_accounts",)),
        ('Auth channels and audience data', ("channel_audience",
                                             "channel_aw_performance",
                                             "video_audience",
                                             "video_aw_performance",
                                             )),
        ('Default',                         ("my_yt_channels", "view_highlights")),
    )

    PERM_LIST = (
        # view section
        "view_trends",
        "view_benchmarks",
        "view_highlights",
        "view_performance",
        "view_media_buying",
        "view_pre_baked_segments",
        "view_create_and_manage_campaigns",
        # video section
        "video_list",
        "video_filter",
        "video_details",
        "video_audience",
        "video_aw_performance",
        # channel section
        "channel_list",
        "channel_filter",
        "channel_details",
        "channel_audience",
        "channel_aw_performance",
        # keyword section
        "keyword_list",
        "keyword_details",
        "keyword_filter",
        # segment section
        "segment_video_all",
        "segment_video_private",
        "segment_channel_all",
        "segment_channel_private",
        "segment_keyword_all",
        "segment_keyword_private",
        # settings section
        "settings_my_aw_accounts",
        "settings_my_yt_channels",
    )
