import csv

from django.db.models import Count
from django.db.models import Sum
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from utils.utils import chunks_generator

from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.constants import Sections

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
        .values("yt_id").annotate(dcount=Count("yt_id"), sum_impressions=Sum("impressions")) \
        .filter(date__year=year, sum_impressions__gt=10) \
        .order_by("dcount") \
        .values_list("yt_id", "sum_impressions")

    manager = data_type.MANAGER(Sections.GENERAL_DATA)

    with open(file_path, mode="w") as csv_file:
        writer = csv.writer(csv_file)

        writer.writerow(["Name", "Youtube link", "Impressions"])

        for chunk in chunks_generator(entities, CHUNK_SIZE):

            impressions_data_entities = [item for item in chunk if item[0]]
            es_entities = manager.get(ids=[entity[0] for entity in impressions_data_entities])

            for impressions_data, entity in zip(impressions_data_entities, es_entities):

                if not entity:
                    continue

                writer.writerow([
                    entity.general_data.title,
                    data_type.YOUTUBE_LINK_TEMPLATE.format(entity.main.id),
                    impressions_data[1]
                ])


