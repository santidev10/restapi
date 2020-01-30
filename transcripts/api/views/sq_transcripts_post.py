import json
from datetime import datetime

from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

from es_components.managers.video import VideoManager
from es_components.models.video import Video
from es_components.constants import Sections
from transcripts.models import SQTranscript
from utils.transform import populate_video_custom_captions


class SQTranscriptsPostApiView(RetrieveUpdateDestroyAPIView):
    def post(self, request):
        # Post Request takes a body, which is one dictionary, with the key being video_id and value being transcript
        body = json.loads(request.data)
        manager = VideoManager(sections=Sections.CUSTOM_CAPTIONS, upsert_sections=Sections.CUSTOM_CAPTIONS)
        video_ids = [vid_id for vid_id in body]
        videos = manager.get(video_ids)
        try:
            for video in videos:
                video_id = video.main.id
                transcript = body[video_id]
                sq_transcript = SQTranscript.get_or_create(video_id)
                sq_transcript.transcript = transcript
                sq_transcript.retrieved = datetime.now()
                sq_transcript.save()
                populate_video_custom_captions(video, [transcript], ["en"], "SQ")
        except Exception as e:
            raise ValidationError(e)
        pass
