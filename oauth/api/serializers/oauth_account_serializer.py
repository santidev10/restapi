from rest_framework import serializers

from oauth.constants import OAuthData


class OAuthAccountSerializer(serializers.Serializer):
    email = serializers.CharField(read_only=True)
    id = serializers.IntegerField(read_only=True)
    is_enabled = serializers.BooleanField(required=False)
    synced = serializers.BooleanField(read_only=True)
    # If OAuthAccount has placed sync script on Google Ads and has synced successfully with ViewIQ
    segment_sync_script_is_verified = serializers.SerializerMethodField()

    def get_segment_sync_script_is_verified(self, obj):
        """
        If obj.data[OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP] is a string value, then it is a timestamp and the oauth
        account has not been synced with Google Ads for the segment app Build feature

        If it is True, then it has been successfully synced in SegmentGadsSyncAPIView
        """
        try:
            script_verified = obj.data[OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP] is True
        except (KeyError, TypeError):
            script_verified = False
        return script_verified

    def update(self, instance, validated_data):
        for key, value in validated_data.items():
            setattr(instance, key, value)
        instance.save()
        return instance
