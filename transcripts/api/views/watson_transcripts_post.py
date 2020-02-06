import json
from datetime import datetime

from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

# from audit_tool.models import AuditLanguage
from es_components.managers.video import VideoManager
from es_components.constants import Sections
from transcripts.models import WatsonTranscript
from audit_tool.models import AuditVideoTranscript
from brand_safety.languages import LANG_CODES, LANGUAGES
from utils.transform import populate_video_custom_captions


class WatsonTranscriptsPostApiView(RetrieveUpdateDestroyAPIView):
    def post(self, request):
        # Post Request takes a body, which is one dictionary, with the key being video_id and value being a dictionary
        # with the transcript and language identified by Watson, if provided
        body = json.loads(request.data)
        manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, Sections.GENERAL_DATA),
                               upsert_sections=(Sections.CUSTOM_CAPTIONS, Sections.GENERAL_DATA))
        video_ids = [vid_id for vid_id in body]
        videos = manager.get(video_ids)
        try:
            for video in videos:
                video_id = video.main.id
                try:
                    language = video.general_data.language or "English"
                    lang_code = video.general_data.lang_code or LANG_CODES[language] or "en"
                except Exception:
                    language = "English"
                    lang_code = "en"
                watson_data = body[video_id]
                transcript = watson_data['transcript']
                watson_language = None
                try:
                    watson_language = watson_data['language']
                    if watson_language in LANG_CODES:
                        pass
                    else:
                        try:
                            watson_language = LANGUAGES[watson_language]
                        except Exception:
                            watson_language = None
                except Exception:
                    pass

                if watson_language:
                    lang_code = LANG_CODES[watson_language]
                    video.populate_general_data(language=watson_language, lang_code=lang_code)
                watson_transcript = WatsonTranscript.get_or_create(video_id)
                watson_transcript.transcript = transcript
                # try:
                #     watson_transcript.language = AuditLanguage.objects.get(lang_code)
                # except Exception:
                #     pass
                watson_transcript.retrieved = datetime.now()
                watson_transcript.save()
                AuditVideoTranscript.get_or_create(video_id=video_id, language=lang_code,
                                                   transcript=transcript)
                populate_video_custom_captions(video, [transcript], [lang_code], "Watson")
                manager.upsert([video])
        except Exception as e:
            raise ValidationError(e)
        pass
