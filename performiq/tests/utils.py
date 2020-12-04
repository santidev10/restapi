def get_params(params):
    default = dict(
        active_view_viewability=0,
        average_cpv=0,
        average_cpm=0,
        content_categories=[],
        content_quality=[],
        content_type=[],
        ctr=0,
        exclude_content_categories=[],
        languages=[],
        score_threshold=0,
        video_view_rate=0,
        video_quartile_100_rate=0,
    )
    default.update(params)
    return default
