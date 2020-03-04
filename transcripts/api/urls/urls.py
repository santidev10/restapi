"""
Transcripts api urls module
"""
from django.conf.urls import url

from transcripts.api.views import WatsonTranscriptsPostApiView

from .names import TranscriptsPathName

urlpatterns = [
    url(r"transcripts/watson_transcripts/post/$", WatsonTranscriptsPostApiView.as_view(),
        name=TranscriptsPathName.WATSON_TRANSCRIPTS)
]
