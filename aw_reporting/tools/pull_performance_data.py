import csv

from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Count
from django.db.models import Sum

from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import get_average_cpm
from aw_reporting.models import get_average_cpv
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from utils.utils import chunks_generator

CHUNK_SIZE = 1000


class PullDataTools:
    class Channel:
        MANAGER = ChannelManager
        DB_MODEL = YTChannelStatistic
        YOUTUBE_LINK_TEMPLATE = "https://www.youtube.com/channel/{}"

    class Video:
        MANAGER = VideoManager
        DB_MODEL = YTVideoStatistic
        YOUTUBE_LINK_TEMPLATE = "https://www.youtube.com/watch?v={}"


def pull_performance_data(year, data_type, file_path):
    entities = data_type.DB_MODEL.objects \
        .values("yt_id").annotate(dcount=Count("yt_id"), sum_impressions=Sum("impressions"),
                                  sum_views=Sum("video_views"), sum_clicks=Sum("clicks"), sum_cost=Sum("cost"),
                                  product_types=ArrayAgg("ad_group__type", distinct=True)) \
        .filter(date__year=year, sum_impressions__gt=10) \
        .order_by("dcount") \
        .values_list("yt_id", "sum_impressions", "sum_views", "sum_clicks", "sum_cost", "product_types")

    manager = data_type.MANAGER(Sections.GENERAL_DATA)

    with open(file_path, mode="w") as csv_file:
        writer = csv.writer(csv_file)

        writer.writerow(["Name", "Youtube link", "Impressions", "Views", "Total Clicks", "Cost", "CPV", "CPM"])

        for chunk in chunks_generator(entities, CHUNK_SIZE):

            entities_stats_data = [item for item in chunk if item[0]]
            es_entities = manager.get(ids=[entity[0] for entity in entities_stats_data])

            for entity_data, entity in zip(entities_stats_data, es_entities):

                if not entity:
                    continue

                _, impressions, views, clicks, cost, product_types = entity_data

                writer.writerow([
                    entity.general_data.title,
                    data_type.YOUTUBE_LINK_TEMPLATE.format(entity.main.id),
                    impressions,
                    views,
                    clicks,
                    cost,
                    get_average_cpv(video_views=views, cost=cost),
                    get_average_cpm(impressions=impressions, cost=cost),
                    ", ".join(product_types)
                ])
