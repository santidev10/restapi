from brand_safety.auditors.utils import AuditUtils
from es_components.constants import Sections
from es_components.managers.transcript import TranscriptManager


class VideoTranscriptSerializerContextMixin:

    def get_transcripts_serializer_context(self, video_ids: list):
        """
        gets a transcripts by video_id map for a VideoSerializer context:
            e.g.: {"123": [Transcript(1), ...]}
        :param video_ids:
        :return:
        """
        transcripts_manager = TranscriptManager(sections=[Sections.VIDEO, Sections.TEXT, Sections.GENERAL_DATA])
        transcripts = transcripts_manager.get_by_video_ids(video_ids=video_ids)
        return AuditUtils.map_transcripts_by_video_id(transcripts=transcripts)
