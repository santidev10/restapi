import logging

from django.core.management.base import BaseCommand

from elasticsearch_dsl import Q
from es_components.constants import Sections
from es_components.managers import VideoManager

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        q1 = Q(
            {
                "bool": {
                    "must": {
                        "exists": {
                            "field": "task_us_data"
                        }
                    }
                }
            }
        )
        q2 = Q(
            {
                "bool": {
                    "must": {
                        "range": {
                            "task_us_data.created_at": {
                                "gte": "2020-07-03T00:00:00"
                            }
                        }
                    }
                }
            }
        )
        q3 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.iab_categories"
                        }
                    }
                }
            }
        )
        q4 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.is_safe"
                        }
                    }
                }
            }
        )
        q5 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.is_user_generated_content"
                        }
                    }
                }
            }
        )
        q6 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.scalable"
                        }
                    }
                }
            }
        )
        q7 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.lang_code"
                        }
                    }
                }
            }
        )
        q8 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.age_group"
                        }
                    }
                }
            }
        )
        q9 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.content_type"
                        }
                    }
                }
            }
        )
        q10 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.content_quality"
                        }
                    }
                }
            }
        )
        q11 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.gender"
                        }
                    }
                }
            }
        )
        q12 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.brand_safety"
                        }
                    }
                }
            }
        )
        q13 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.last_vetted_at"
                        }
                    }
                }
            }
        )
        q14 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "task_us_data.mismatched_language"
                        }
                    }
                }
            }
        )
        videos_query = q1 + q2 + q3 + q4 + q5 + q6 + q7 + q8 + q9 + q10 + q11 + q12 + q13 + q14
        sections_to_remove = [Sections.TASK_US_DATA]
        manager = VideoManager(Sections.TASK_US_DATA)
        logger.info("Removing empty task_us_data sections for Videos...")
        manager.remove_sections(videos_query, sections_to_remove)
        logger.info("Finish removing empty task_us_data sections for Videos.")
