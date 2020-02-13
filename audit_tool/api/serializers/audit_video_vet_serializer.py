from django.utils import timezone

from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import SerializerMethodField

from audit_tool.models import AuditVideoVet
from audit_tool.models import AuditChannel
from audit_tool.models import get_hash_name
from es_components.models import Channel
from es_components.models import Video
from es_components.constants import Sections
from es_components.managers import ChannelManager
from audit_tool.api.serializers.audit_vet_base_serializer import AuditVetBaseSerializer


class AuditVideoVetSerializer(AuditVetBaseSerializer):
    """
    age_group, channel_type, gender, and brand_safety values are stored as id values
    """
    general_data_language_field = "language"
    document_model = Video

    # Postgres fields
    vetting_history = SerializerMethodField()
    segment_title = SerializerMethodField()
    url = SerializerMethodField()
    checked_out_at = DateTimeField(required=False, allow_null=True)
    suitable = BooleanField(required=False)
    processed = DateTimeField(required=False)
    processed_by_user_id = IntegerField(required=False)
    language_code = CharField(required=False) # Field for saving vetting item

    def get_url(self, doc):
        url = f"https://www.youtube.com/video/{doc.main.id}/"
        return url

    def get_vetting_history(self, doc):
        """
        Retrieve vetting history of all AuditVideoVet items with FK to AuditChannel
        Only retrieve if serializing with Elasticsearch document
        :param doc: Elasticsearch document
        :return: dict
        """
        history = None
        if hasattr(doc, "main"):
            video_id_hash = get_hash_name(doc.main.id)
            vetting_items = AuditVideoVet.objects\
                .select_related("video", "video__auditvideometa")\
                .filter(video__video_id_hash=video_id_hash, processed__isnull=False)
            history = [{
                "data": f"{item.video.auditvideometa.name} - {item.processed.strftime('%b %d %Y')}",
                "suitable": item.clean
            } for item in vetting_items]
        # Set bool for get_language method to return correct language field
        # If has never been vetted before, should return general data language, else vetted language
        self.has_vetting_history = bool(history)
        return history

    def save(self, **kwargs):
        """
        Save values on AuditVideoVet instance, FK AuditVideoMeta, and Elasticsearch task_us_data section
        :param kwargs: dict
        :return:
        """
        if not self.instance:
            raise ValueError("To save serializer, must be provided instance object"
                             "during instantiation.")
        video_meta = self.instance.video.auditvideometa
        video_id = video_meta.video.video_id
        self._save_vetting_item()
        blacklist_categories = self.save_brand_safety(video_id)
        try:
            channel_id = video_meta.video.channel.channel_id
        except AttributeError:
            channel_id = None
        self.save_elasticsearch(video_id, blacklist_categories)
        if channel_id:
            self._update_channel(channel_id)

    def _save_vetting_item(self):
        """
        Save only required fields for database
        :param channel_meta: AuditChannelMeta
        :return: None
        """
        data = {
            "clean": self.validated_data["suitable"],
            "checked_out_at": None,
            "processed": timezone.now(),
            "processed_by_user_id": self.validated_data["processed_by_user_id"],
        }
        for key, value in data.items():
            setattr(self.instance, key, value)
        self.instance.save(update_fields=list(data.keys()))

    def _update_channel(self, channel_id):
        """
        Update channel monetization
        :param channel_id: str
        :return: None
        """
        if channel_id and self.validated_data["monetization"]["is_monetizable"] is True:
            try:
                channel_meta = self.instance.channel.auditchannelmeta
                channel_meta.monetised = True
                channel_meta.save()
            except AttributeError:
                pass
            manager = ChannelManager(sections=(Sections.MONETIZATION,))
            channel_doc = Channel(channel_id)
            channel_doc.populate_monetization(is_monetizable=True)
            manager.upsert([channel_doc])
