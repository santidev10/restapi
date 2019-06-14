import os
from email.mime.image import MIMEImage

import pytz
from django.core.paginator import Paginator
from django.core.paginator import EmptyPage
from django.http import Http404
from django.http import StreamingHttpResponse
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from rest_framework.generics import GenericAPIView
from rest_framework.generics import ListAPIView
from rest_framework.generics import ListCreateAPIView
from rest_framework.generics import RetrieveAPIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from channel.api.views import ChannelListApiView
from segment.api.serializers import PersistentSegmentSerializer
from segment.api.serializers import SegmentSerializer
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.models.persistent.constants import PersistentSegmentType
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from segment.utils import get_persistent_segment_model_by_type
from segment.utils import get_segment_model_by_type
from segment.utils import get_persistent_segment_connector_config_by_type
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException
from userprofile.models import UserProfile
from userprofile.permissions import PermissionGroupNames
from utils.api_paginator import CustomPageNumberPaginator
from utils.permissions import user_has_permission
from segment.models.persistent import PersistentSegmentFileUpload


class PersistentSegmentRetrieveApiView(DynamicPersistentModelViewMixin, RetrieveAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )
