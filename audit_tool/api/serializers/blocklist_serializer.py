from rest_framework import serializers


class BlocklistSerializer(serializers.Serializer):
    EXPORT_FIELDS = ("title", "url", "date_added", "added_by_user", "blocked_count", "unblocked_count")

    title = serializers.CharField(source="general_data.title")
    url = serializers.SerializerMethodField("get_url")
    date_added = serializers.SerializerMethodField()
    added_by_user = serializers.SerializerMethodField()
    blocked_count = serializers.SerializerMethodField()
    unblocked_count = serializers.SerializerMethodField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.context["blacklist_data"]
        except KeyError:
            raise KeyError("You must provide blacklist_data as a dictionary of video or channel id, BlacklistItem key, "
                           "values in serializer context.")

    def get_date_added(self,  obj):
        added = self._get(obj.main.id, "updated_at")
        return added

    def get_added_by_user(self, obj):
        email = self._get(obj.main.id, "email")
        return email

    def get_url(self, obj):
        item_id = obj.main.id
        resource = "/v/?watch=" if len(item_id) < 20 else "/channel/"
        url = f"https://www.youtube.com{resource}{item_id}"
        return url

    def get_blocked_count(self, obj):
        blocked_count = self._get(obj.main.id, "blocked_count")
        return blocked_count

    def get_unblocked_count(self, obj):
        unblocked_count = self._get(obj.main.id, "unblocked_count")
        return unblocked_count

    def _get(self, item_id, attrname):
        try:
            value = getattr(self.context["blacklist_data"].get(item_id), attrname)
        except AttributeError:
            value = None
        return value
