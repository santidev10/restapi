import logging
from saas import celery_app
import requests
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q

from es_components.connections import init_es_connection
from bs4 import BeautifulSoup as bs
from audit_tool.models import AuditVideoTranscript
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from es_components.constants import Sections
from utils.transform import populate_video_custom_captions
from utils.lang import replace_apostrophes
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock

logger = logging.getLogger(__name__)

LOCK_NAME = 'custom_transcripts'


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def pull_custom_transcripts():
    try:
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        init_es_connection()
        logger.debug("Pulling custom transcripts.")
        unparsed_vids = get_unparsed_vids()
        vid_ids = set([vid.main.id for vid in unparsed_vids])
        counter = 0
        transcripts_counter = 0
        video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                     upsert_sections=(Sections.CUSTOM_CAPTIONS,))
        for vid_id in vid_ids:
            vid_obj = video_manager.get_or_create([vid_id])[0]
            transcript_soup = get_video_soup(vid_id)
            transcript_text = replace_apostrophes(transcript_soup.text) if transcript_soup else ""
            if transcript_text != "":
                AuditVideoTranscript.get_or_create(video_id=vid_id, language="en", transcript=str(transcript_soup))
                logger.debug("VIDEO WITH ID {} HAS A CUSTOM TRANSCRIPT.".format(vid_id))
                transcripts_counter += 1
            else:
                AuditVideoTranscript.get_or_create(video_id=vid_id, language=None)
            populate_video_custom_captions(vid_obj, [transcript_text], ['en'])
            video_manager.upsert([vid_obj])
            counter += 1
            logger.debug("Parsed video with id: {}".format(vid_id))
            logger.debug("Number of videos parsed: {}".format(counter))
            logger.debug("Number of transcripts retrieved: {}".format(transcripts_counter))
        unlock(LOCK_NAME)
        logger.debug("Finished pulling 1,000 custom transcripts.")
    except Exception:
        pass


def get_video_soup(vid_id):
    transcript_url = "http://video.google.com/timedtext?lang=en&v="
    vid_transcript_url = transcript_url + vid_id
    transcript_response = requests.get(vid_transcript_url)
    if transcript_response.status_code == 200:
        soup = bs(transcript_response.text, "xml")
        return soup
    else:
        return None


def get_unparsed_vids():
    s = Search(using='default')
    s = s.index(Video.Index.name)

    # Get English Videos Query
    q1 = Q(
        {
            "term": {
                "general_data.language": {
                    "value": "English"
                }
            }
        }
    )
    # Get Videos with no captions
    q2 = Q(
        {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "captions"
                    }
                }
            }
        }
    )
    # Get Videos with no custom_captions.transcripts_checked
    q3 = Q(
        {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "custom_captions.transcripts_checked"
                    }
                }
            }
        }
    )
    s = s.query(q1).query(q2).query(q3)
    s = s.sort({"stats.views": {"order": "desc"}})
    s = s[:1000]
    return s.execute()
