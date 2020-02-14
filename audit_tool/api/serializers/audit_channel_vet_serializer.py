from django.utils import timezone

from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import SerializerMethodField

from audit_tool.models import AuditChannelVet
from audit_tool.models import get_hash_name
from es_components.models import Channel
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import Sections
from audit_tool.api.serializers.audit_vet_base_serializer import AuditVetBaseSerializer


class AuditChannelVetSerializer(AuditVetBaseSerializer):
    """
    age_group, channel_type, gender, and brand_safety values are stored as id values
    """
    general_data_language_field = "top_language"
    document_model = Channel

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
        url = f"https://www.youtube.com/channel/{doc.main.id}/"
        return url

    def get_vetting_history(self, doc):
        """
        Retrieve vetting history of all AuditChannelVet items with FK to AuditChannel
        Only retrieve if serializing with Elasticsearch document
        :param doc: Elasticsearch document
        :return: dict
        """
        history = None
        if hasattr(doc, "main"):
            channel_id_hash = get_hash_name(doc.main.id)
            vetting_items = AuditChannelVet.objects\
                .filter(channel__channel_id_hash=channel_id_hash, processed__isnull=False)\
                .select_related("channel__auditchannelmeta")
            history = [{
                "data": f"{item.channel.auditchannelmeta.name} - {item.processed.strftime('%b %d %Y')}",
                "suitable": item.clean
            } for item in vetting_items]
        # Set bool for get_language method to return correct language field
        self.has_vetting_history = bool(history)
        return history

    def save(self, **kwargs):
        """
        Save values on AuditChannelVet instance, FK AuditChannelMeta, and Elasticsearch task_us_data section
        If channel is monetized, will also update all videos monetization with _update_videos method
        :param kwargs: dict
        :return:
        """
        if not self.instance:
            raise ValueError("To save serializer, must be provided instance object"
                             "during instantiation.")
        channel_meta = self.instance.channel.auditchannelmeta
        channel_id = channel_meta.channel.channel_id

        self._save_vetting_item(channel_meta, channel_id)
        blacklist_categories = self.save_brand_safety(channel_id)
        self.save_elasticsearch(channel_id, blacklist_categories)
        self._update_videos(channel_id)

    def _save_vetting_item(self, channel_meta, channel_id):
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
        if self.validated_data["monetization"]["is_monetizable"] is True:
            channel_meta.monetised = True
            channel_meta.save()

    def _update_videos(self, channel_id):
        """
        Update all channel videos
        :param channel_id: str
        :return: None
        """
        if self.validated_data["monetization"]["is_monetizable"] is True:
            # Update all channel videos monetization
            query = QueryBuilder().build().must().term().field(f"{Sections.CHANNEL}.id").value(channel_id).get()
            VideoManager(sections=(Sections.MONETIZATION,)).update_monetization(query, True)
