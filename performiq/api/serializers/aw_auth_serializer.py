from rest_framework.serializers import ModelSerializer

from performiq.models import OAuthAccount


class AWAuthSerializer(ModelSerializer):

    class Meta:
        model = OAuthAccount
        fields = ("id", "email", "name", "created_at", "updated_at")
