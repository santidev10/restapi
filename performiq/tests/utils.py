from unittest import mock
from types import SimpleNamespace
from typing import List

from performiq.api.serializers import IQCampaignSerializer
from performiq.analyzers.executor_analyzer import ExecutorAnalyzer
from performiq.analyzers import ChannelAnalysis
from es_components.managers import ChannelManager
from es_components.models import Channel


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
        score_threshold=1,
        video_view_rate=0,
        video_quartile_100_rate=0,
        name="test",
        user_id=0,
    )
    default.update(params)
    serializer = IQCampaignSerializer(data=default)
    serializer.is_valid(raise_exception=True)
    return serializer.validated_data


def get_test_analyses(channel_docs: List[Channel]):
    analyses = [
        ChannelAnalysis(c.main.id, data={})
        for c in channel_docs
    ]

    with mock.patch.object(ChannelManager, "get", return_value=channel_docs),\
            mock.patch.object(ExecutorAnalyzer, "_prepare_data"):
        mock_campaign = SimpleNamespace()
        mock_campaign.params = {}
        executor = ExecutorAnalyzer(mock_campaign)
        data = executor._merge_es_data(analyses)
    return data
