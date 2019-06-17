from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.serializers import ValidationError

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.api.views.pagination import BrandSafetyPaginator
from brand_safety.models import BadWord

class BadWordHistoryApiView(ListAPIView):
    permission_classes = (IsAdminUser,)

