import logging

from django.core.management.base import BaseCommand

from elasticsearch_dsl import Q
from es_components.constants import Sections
from es_components.managers import VideoManager

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        videos_query = Q(
            {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "exists": {
                                    "field": "task_us_data"
                                }
                            },
                            {
                                "range": {
                                    "task_us_data.created_at": {
                                        "gte": "2020-07-03T00:00:00"
                                    }
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "exists": {
                                    "field": "task_us_data.iab_categories"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.is_safe"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.is_user_generated_content"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.scalable"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.lang_code"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.age_group"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.content_type"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.content_quality"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.gender"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.brand_safety"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.last_vetted_at"
                                }
                            },
                            {
                                "exists": {
                                    "field": "task_us_data.mismatched_language"
                                }
                            }
                        ]
                    }
                },
                "sort": {
                    "task_us_data.created_at": {
                        "order": "asc"
                    }
                }
            }
        )
        sections_to_remove = [Sections.TASK_US_DATA]
        manager = VideoManager(Sections.TASK_US_DATA)
        logger.info("Removing empty task_us_data sections for Videos...")
        manager.remove_sections(videos_query, sections_to_remove)
        logger.info("Finish removing empty task_us_data sections for Videos.")
