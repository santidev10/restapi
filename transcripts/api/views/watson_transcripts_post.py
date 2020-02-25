from django.utils import timezone

from django.conf import settings
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.managers.video import VideoManager
from es_components.constants import Sections
from audit_tool.models import AuditVideoTranscript
from brand_safety.languages import LANG_CODES, LANGUAGES
from utils.transform import populate_video_custom_captions
from transcripts.tasks.rescore_brand_safety import rescore_brand_safety_videos


class WatsonTranscriptsPostApiView(RetrieveUpdateDestroyAPIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        # Post Request takes a body, which is a dictionary, with the keys "token" and "transcripts";
        # "token" is our API token for authentication
        # "transcripts" is a dictionary, with the key being the video_id and value being a dictionary
        # with the transcript and language identified by Watson, if provided
        query_params = request.query_params
        if query_params.get("authorization") != settings.TRANSCRIPTS_API_TOKEN:
            return Response(
                status=HTTP_403_FORBIDDEN,
                data="Invalid authorization token for POST /transcripts."
            )
        transcripts = request.data
        manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, Sections.GENERAL_DATA),
                               upsert_sections=(Sections.CUSTOM_CAPTIONS, Sections.GENERAL_DATA))
        video_ids = [vid_id for vid_id in transcripts]
        videos = manager.get(video_ids, skip_none=True)
        transcripts_ids = []
        try:
            for video in videos:
                video_id = video.main.id
                try:
                    language = video.general_data.language or "English"
                    lang_code = video.general_data.lang_code or LANG_CODES[language] or "en"
                except Exception:
                    language = "English"
                    lang_code = "en"
                watson_data = transcripts[video_id]
                transcript = watson_data['transcript']

                watson_transcript = AuditVideoTranscript.get_or_create(video_id=video_id, language=lang_code,
                                                                       transcript=transcript, source=1)
                watson_transcript.retrieved = timezone.now()
                watson_transcript.save()

                populate_video_custom_captions(video, [transcript], [lang_code], "Watson")
                transcripts_ids.append(video_id)
            manager.upsert(videos)
            rescore_brand_safety_videos.delay(vid_ids=transcripts_ids)
        except Exception as e:
            raise ValidationError(e)
        return Response(
            status=HTTP_200_OK,
            data=f"Stored transcripts for {len(transcripts_ids)} videos: {', '.join(transcripts_ids)}"
        )
