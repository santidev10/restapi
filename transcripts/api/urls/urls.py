"""
Transcripts api urls module
"""
from django.conf.urls import url

from transcripts.api.views import SQTranscriptsPostApiView

from .names import TranscriptsPathName

urlpatterns = [
    url(r"transcripts/sq_transcripts/post/$", SQTranscriptsPostApiView.as_view(), name=TranscriptsPathName.SQ_TRANSCRIPTS)
]
