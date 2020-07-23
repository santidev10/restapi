from django.core.management import BaseCommand

from es_components.query_builder import QueryBuilder
from es_components.managers import ChannelManager, VideoManager
from utils.utils import chunks_generator

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        c_manager = ChannelManager(["task_us_data"])
        v_manager = VideoManager(["task_us_data"])

        with_task_us = QueryBuilder().build().must().exists().field("task_us_data.content_type").get()
        for batch in chunks_generator(c_manager.scan(with_task_us), size=2000):
            batch = list(batch)
            for doc in batch:
                # Must do quality type first as to not change mapping defined in jira
                # Map quality type
                if doc.task_us_data.content_type == 1:
                    doc.task_us_data.quality_type = 1

                elif doc.task_us_data.content_type in [0, 2]:
                    doc.task_us_data.quality_type = 2

                # Map content type
                if doc.task_us_data.content_type == 0:
                    doc.task_us_data.content_type = 1

                elif doc.task_us_data.content_type in [1, 2]:
                    doc.task_us_data.content_type = 0
            c_manager.upsert(batch)


        for batch in chunks_generator(v_manager.scan(with_task_us), size=2000):
            batch = list(batch)
            for doc in batch:
                # Map quality type
                if doc.task_us_data.content_type == 1:
                    doc.task_us_data.quality_type = 1

                elif doc.task_us_data.content_type in [0, 2]:
                    doc.task_us_data.quality_type = 2

                # Map content type
                if doc.task_us_data.content_type == 0:
                    doc.task_us_data.content_type = 1

                elif doc.task_us_data.content_type in [1, 2]:
                    doc.task_us_data.content_type = 0

            v_manager.upsert(batch)
