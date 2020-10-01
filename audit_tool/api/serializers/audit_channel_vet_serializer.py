from django.utils import timezone
from rest_framework.serializers import SerializerMethodField

from audit_tool.api.serializers.audit_vet_base_serializer import AuditVetBaseSerializer
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelVet
from audit_tool.models import get_hash_name
from brand_safety.tasks.channel_update import channel_update
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.query_builder import QueryBuilder


class AuditChannelVetSerializer(AuditVetBaseSerializer):
    """
    age_group, channel_type, gender, and brand_safety values are stored as id values
    """
    data_type = "channel"
    general_data_lang_code_field = "top_lang_code"
    document_model = Channel

    # Postgres fields
    vetting_history = SerializerMethodField()
    segment_title = SerializerMethodField()
    url = SerializerMethodField()

    def __init__(self, *args, **kwargs):
        super(AuditChannelVetSerializer, self).__init__(*args, **kwargs)
        self.has_vetting_history = None
        self.es_manager = ChannelManager((Sections.MAIN, Sections.TASK_US_DATA, Sections.MONETIZATION,
                                          Sections.GENERAL_DATA, Sections.BRAND_SAFETY))

    def get_url(self, doc):
        url = f"https://www.youtube.com/channel/{doc.main.id}/"
        return url

    def get_vetting_history(self, doc):
        """
        Retrieve vetting history of all AuditChannelVet items with FK to AuditChannel and who were vetted as part of
        an audit (we don't want to show vetting history when just a single channel was vetted)
        Only retrieve if serializing with Elasticsearch document since vetting history is only used for client to
            display during vetting
        :param doc: Elasticsearch document
        :return: dict
        """
        history = None
        if hasattr(doc, "main"):
            channel_id_hash = get_hash_name(doc.main.id)
            vetting_items = AuditChannelVet.objects \
                .filter(channel__channel_id_hash=channel_id_hash, processed__isnull=False, audit__isnull=False) \
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
        channel_id = self.instance.channel.channel_id
        try:
            channel_meta = self.instance.channel.auditchannelmeta
        except AuditChannelMeta.DoesNotExist:
            channel_meta = None
        self._save_vetting_item(channel_meta, channel_id)
        self.save_elasticsearch(channel_id)
        self._update_videos(channel_id)

    def _save_vetting_item(self, channel_meta, channel_id):
        """
        Save only required fields for database
        :param channel_meta: AuditChannelMeta
        :return: None
        """
        data = {
            "clean": self.validated_data.get("suitable"),
            "checked_out_at": None,
            "processed": timezone.now(),
            "processed_by_user_id": self.context["user"].id,
        }
        for key, value in data.items():
            setattr(self.instance, key, value)
        self.instance.save(update_fields=list(data.keys()))

    def _update_videos(self, channel_id):
        """
        Update all channel videos
        :param channel_id: str
        :return: None
        """
        if self.validated_data["monetization"].get("is_monetizable") is True:
            # Update all channel videos monetization
            query = QueryBuilder().build().must().term().field(f"{Sections.CHANNEL}.id").value(channel_id).get()
            VideoManager(sections=(Sections.MONETIZATION,)).update_monetization(query, True, conflicts="proceed",
                                                                                wait_for_completion=False)

    def update_brand_safety(self, item_id):
        """ Initiate brand safety update task """
        channel_update.delay(item_id, ignore_vetted_channels=False)
