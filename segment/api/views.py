import os
import re
from email.mime.image import MIMEImage

import pytz
from django.core.paginator import Paginator
from django.core.paginator import EmptyPage
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.db.models import CharField
from django.db.models import Q
from django.db.models import Value
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


class SegmentPaginator(CustomPageNumberPaginator):
    """
    Paginator for segments list
    """
    page_size = 10
    page_size_query_param = "page_size"


class DynamicModelViewMixin(object):
    def dispatch(self, request, segment_type, **kwargs):
        self.model = get_segment_model_by_type(segment_type)
        self.serializer_class.Meta.model = self.model
        return super().dispatch(request, **kwargs)

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        if self.request.user.is_staff:
            queryset = self.model.objects.all()
        elif self.request.user.has_perm('userprofile.view_pre_baked_segments'):
            queryset = self.model.objects.filter(
                Q(owner=self.request.user)
                | ~Q(category='private')
                | Q(shared_with__contains=[self.request.user.email])
            )
        else:
            queryset = self.model.objects.filter(
                Q(owner=self.request.user)
                | Q(shared_with__contains=[self.request.user.email])
            )
        return queryset


class SegmentListCreateApiView(DynamicModelViewMixin, ListCreateAPIView):
    """
    Segment list/create endpoint
    """
    serializer_class = SegmentSerializer
    pagination_class = SegmentPaginator

    default_allowed_sorts = {
        "title",
        "videos",
        "engage_rate",
        "sentiment",
        "created_at",
    }
    allowed_sorts = {
        "channel": default_allowed_sorts.union({"channels"}),
        "keyword": {"competition", "average_cpc", "average_volume"}
    }

    def __validate_filters(self):
        owner_id = self.request.query_params.get("owner_id")
        if owner_id is not None:
            return owner_id == str(self.request.user.id)\
                   or self.request.user.is_staff
        return True

    def do_filters(self, queryset):
        """
        Filter queryset
        """
        filters = {}
        # search
        search = self.request.query_params.get("search")
        if search:
            filters["title__icontains"] = search
        # category
        category = self.request.query_params.get("category")
        if category:
            filters["category"] = category
        owner_id = self.request.query_params.get("owner_id")
        if owner_id:
            filters["owner__id"] = owner_id
        # make filtering
        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def do_sorts(self, queryset):
        """
        Sort queryset
        """
        segment = self.model.segment_type
        allowed_sorts = self.allowed_sorts.get(segment,
                                               self.default_allowed_sorts)

        def get_sort_prefix():
            """
            Define ascending or descending sort
            """
            reverse = "-"
            ascending = self.request.query_params.get("ascending")
            if ascending == "1":
                reverse = ""
            return reverse

        sort = self.request.query_params.get("sort_by")
        if sort in allowed_sorts:
            queryset = queryset.order_by("{}{}".format(
                get_sort_prefix(), sort))
        return queryset

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        queryset = super().get_queryset()
        queryset = self.do_filters(queryset)
        queryset = self.do_sorts(queryset)
        return queryset

    def paginate_queryset(self, queryset):
        """
        Processing flat query param
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)

    def get(self, request, *args, **kwargs):
        if not self.__validate_filters():
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data={"error": "invalid filter(s)"})
        return super(SegmentListCreateApiView, self).get(
            request, *args, **kwargs)


class SegmentRetrieveUpdateDeleteApiView(DynamicModelViewMixin,
                                         RetrieveUpdateDestroyAPIView):
    serializer_class = SegmentSerializer

    def delete(self, request, *args, **kwargs):
        segment = self.get_object()
        user = request.user
        if not (user.is_staff or segment.owner == user):
            return Response(status=HTTP_403_FORBIDDEN)
        return super().delete(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        """
        Allow partial update
        """
        segment = self.get_object()
        user = request.user

        if not (user.is_staff or segment.owner == user):
            return Response(status=HTTP_403_FORBIDDEN)

        serializer_context = {"request": request}

        serializer = self.serializer_class(
            instance=segment, data=request.data,
            context=serializer_context, partial=True
        )
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()

        response_data = self.serializer_class(
            segment,
            context=serializer_context
        ).data
        return Response(response_data)


class SegmentShareApiView(DynamicModelViewMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = SegmentSerializer

    def put(self, request, *args, **kwargs):
        segment = self.get_object()
        user = request.user
        shared_with = request.data.get('shared_with')

        # reject if request user has no permissions
        if not (user.is_staff or segment.owner == user):
            return Response(status=HTTP_403_FORBIDDEN)

        # send invitation email if user exists, else send registration email
        self.proceed_emails(segment, shared_with)

        # return saved segment
        serializer_context = {"request": request}
        response_data = self.serializer_class(
            segment,
            context=serializer_context
        ).data
        return Response(data=response_data, status=HTTP_200_OK)

    def proceed_emails(self, segment, emails):
        sender = settings.SENDER_EMAIL_ADDRESS
        message_from = self.request.user.get_full_name()
        exist_emails = segment.shared_with
        host = self.request.get_host()
        subject = "ViewIQ > You have been added as collaborator"
        segment_url = "https://{host}/media_planning/{segment_type}s/{segment_id}"\
                      .format(host=host,
                              segment_type=segment.segment_type,
                              segment_id=segment.id)
        context = dict(
            host=host,
            message_from=message_from,
            segment_url=segment_url,
            segment_title=segment.title,
        )
        # collect only new emails for current segment
        emails_to_iterate = [e for e in emails if e not in exist_emails]

        for email in emails_to_iterate:
            try:
                user = UserProfile.objects.get(email=email)
                context['name'] = user.get_full_name()
                to = user.email
                html_content = render_to_string(
                    "new_enterprise_collaborator.html",
                    context
                )
                self.send_email(html_content, subject, sender, to)
                # provide access to segments for collaborator
                user.add_custom_user_group(PermissionGroupNames.MEDIA_PLANNING)

            except UserProfile.DoesNotExist:
                to = email
                html_content = render_to_string(
                    "new_collaborator.html",
                    context)
                self.send_email(html_content, subject, sender, to)

        # update collaborators list
        segment.shared_with = emails
        segment.save()

    def send_email(self, html_content, subject, sender, to):
        text = strip_tags(html_content)
        msg = EmailMultiAlternatives(subject, text, sender, [to])
        msg.attach_alternative(html_content, "text/html")
        for f in ['bg.png', 'cf_logo_wt_big.png', 'img.png', 'logo.gif']:
            fp = open(os.path.join('segment/templates/', f), 'rb')
            msg_img = MIMEImage(fp.read())
            fp.close()
            msg_img.add_header('Content-ID', '<{}>'.format(f))
            msg.attach(msg_img)
        msg.send()


class SegmentDuplicateApiView(DynamicModelViewMixin, GenericAPIView):
    serializer_class = SegmentSerializer

    def post(self, request, pk):
        """
        Make a copy of segment and attach to user
        """
        segment = self.get_object()
        duplicated_segment = segment.duplicate(request.user)

        response_data = self.serializer_class(
            duplicated_segment,
            context={"request": request}
        ).data

        return Response(response_data, status=HTTP_201_CREATED)


class SegmentSuggestedChannelApiView(DynamicModelViewMixin, GenericAPIView):
    serializer_class = SegmentSerializer
    connector = Connector()

    def get(self, request, *args, **kwargs):
        segment = self.get_object()
        query_params = self.request.query_params
        query_params._mutable = True
        response_data = []

        if segment.top_recommend_channels:
            try:
                query_params['ids'] = ','.join(
                    segment.top_recommend_channels[:100])
                response_data = self.connector.get_channel_list(query_params)
            except SingleDatabaseApiConnectorException:
                return Response(status=HTTP_408_REQUEST_TIMEOUT)
        if response_data:
            ChannelListApiView.adapt_response_data(response_data, request.user)
        return Response(response_data)


class DynamicPersistentModelViewMixin(object):
    def dispatch(self, request, segment_type, **kwargs):
        self.model = get_persistent_segment_model_by_type(segment_type)
        if hasattr(self, "serializer_class"):
            self.serializer_class.Meta.model = self.model
        return super().dispatch(request, **kwargs)

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        queryset = self.model.objects.all().order_by("title")
        return queryset


class PersistentSegmentListApiView(DynamicPersistentModelViewMixin, ListAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    def get_queryset(self):
        queryset = super().get_queryset().filter(
                                            Q(title__in=PersistentSegmentTitles.MASTER_BLACKLIST_SEGMENT_TITLES)
                                            | Q(title__in=PersistentSegmentTitles.MASTER_WHITELIST_SEGMENT_TITLES)
                                            | Q(category=PersistentSegmentCategory.WHITELIST)
                                            | Q(category=PersistentSegmentCategory.TOPIC)
                                         )
        return queryset

    def finalize_response(self, request, response, *args, **kwargs):
        formatted_response = {
            "items": []
        }
        for item in response.data.get("items", []):
            if item.get("title") in PersistentSegmentTitles.MASTER_BLACKLIST_SEGMENT_TITLES:
                formatted_response["master_blacklist"] = item

            if self.model.segment_type == PersistentSegmentType.CHANNEL and item.get("title") in PersistentSegmentTitles.CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE:
                formatted_response["master_whitelist"] = item
            elif item.get("title") in PersistentSegmentTitles.MASTER_WHITELIST_SEGMENT_TITLES:
                formatted_response["master_whitelist"] = item

            if item.get("title") not in PersistentSegmentTitles.ALL_MASTER_SEGMENT_TITLES and item.get("title") not in PersistentSegmentTitles.CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE:
                formatted_response["items"].append(item)

            if not item.get("thumbnail_image_url"):
                item["thumbnail_image_url"] = S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL

        for item in formatted_response["items"]:
            # remove "Channels " or "Videos " prefix
            prefix = "{}s ".format(item.get("segment_type").capitalize())
            if item.get("title", prefix).startswith(prefix):
                item["title"] = item.get("title", "")[len(prefix):]

        # response.data["items"] = items
        response.data = formatted_response
        return super().finalize_response(request, response, *args, **kwargs)


class PersistentMasterSegmentsListApiView(ListAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        user_has_permission("userprofile.view_white_lists"),
    )

    def get_queryset(self):
        channels_segment_queryset = PersistentSegmentChannel.objects\
            .filter(title__in=[PersistentSegmentTitles.CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
                               PersistentSegmentTitles.CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE])\
            .annotate(segment_type=Value(PersistentSegmentType.CHANNEL, output_field=CharField()))

        videos_segment_queryset = PersistentSegmentVideo.objects\
            .filter(title=PersistentSegmentTitles.VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE)\
            .annotate(segment_type=Value(PersistentSegmentType.VIDEO, output_field=CharField()))

        return videos_segment_queryset.union(channels_segment_queryset)


class PersistentSegmentRetrieveApiView(DynamicPersistentModelViewMixin, RetrieveAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )


class PersistentSegmentExportApiView(DynamicPersistentModelViewMixin, APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    def get(self, request, pk, *_):
        try:
            segment = self.get_queryset().get(pk=pk)
            content_generator = segment.get_s3_export_content().iter_chunks()
        except segment.__class__.DoesNotExist:
            raise Http404
        response = StreamingHttpResponse(
            content_generator,
            content_type=segment.export_content_type,
            status=HTTP_200_OK,
        )
        filename = self.get_filename(segment)
        response["Content-Disposition"] = "attachment; filename='{}'".format(filename)
        return response

    @staticmethod
    def get_filename(segment):
        try:
            date = re.search("([0-9]{4}-[0-9]{2}-[0-9]{2})", segment.s3_filename).group()
            if date is None:
                filename = "{}.csv".format(segment.title)
            else:
                filename = "{} - {}.csv".format(segment.title, date)
        except AttributeError:
            filename = "{}.csv".format(segment.title)
        return filename


class PersistentSegmentPreviewAPIView(APIView):
    """
    View to provide preview data for persistent segments
    """
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )
    MAX_PAGE_SIZE = 10
    DEFAULT_PAGE_SIZE = 5
    MAX_ITEMS = 100

    def get(self, request, **kwargs):
        """
        Provides paginated persistent segment preview data
        :param request:
            query_params: page (int), size (int)
        :param kwargs:
            segment_type (str) -> video / channel
        :return:
        """
        preview_type_id_mapping = {
            PersistentSegmentType.CHANNEL: "channel_id",
            PersistentSegmentType.VIDEO: "video_id"
        }
        page = request.query_params.get("page", 1)
        size = request.query_params.get("size", self.DEFAULT_PAGE_SIZE)
        segment_type = kwargs["segment_type"]
        item_id_key = preview_type_id_mapping.get(segment_type)
        if item_id_key is None:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid segment type: {}".format(segment_type))
        try:
            page = int(page)
        except ValueError:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid page number: {}".format(page))
        try:
            size = int(size)
        except ValueError:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid page size number: {}".format(page))
        if page <= 0:
            page = 1
        if size <= 0:
            size = self.DEFAULT_PAGE_SIZE
        elif size >= self.MAX_PAGE_SIZE:
            size = self.MAX_PAGE_SIZE
        segment_model = get_persistent_segment_model_by_type(segment_type)
        try:
            segment = segment_model.objects.get(id=kwargs["pk"])
        except segment_model.DoesNotExist:
            raise Http404
        related_items = segment.related.select_related("segment").order_by("related_id")[:self.MAX_ITEMS]
        paginator = Paginator(related_items, size)
        try:
            preview_page = paginator.page(page)
        except EmptyPage:
            page = paginator.num_pages
            preview_page = paginator.page(page)
        related_ids = [item.related_id for item in preview_page.object_list]
        # Helper function to set SDB connector config since Channel and Video SDB querying is similar
        config = get_persistent_segment_connector_config_by_type(segment_type, related_ids)
        if config is None:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid segment type: {}".format(segment_type))
        connector_method = config.pop("method")
        response = connector_method(config)
        response_items = {
            item[item_id_key]: item for item in response.get("items")
        }
        preview_data = []
        for item in preview_page.object_list.values("related_id", "title", "category", "details", "thumbnail_image_url"):
            if response_items.get(item["related_id"]):
                # Map Cassandra item video_id, channel_id fields to just id
                data = response_items[item["related_id"]]
                item_id = data.pop(item_id_key)
                data["id"] = item_id
            else:
                # Map Postgres data to Cassandra structure
                data = self._map_segment_data(item, segment_type)
            preview_data.append(data)
        result = {
            "items": preview_data,
            "items_count": len(preview_data),
            "current_page": page,
            "max_page": paginator.num_pages,
        }
        return Response(status=HTTP_200_OK, data=result)

    @staticmethod
    def _map_segment_data(data, segment_type):
        """
        Maps Postgres persistent segment data to Singledb formatted data for client
        :param data: dict
        :param segment_type: str
        :return:
        """
        mapped_data = {
            "id": data["related_id"],
            "title": data.get("title"),
            "category": data.get("category"),
            "views": data["details"].get("views"),
            "likes": data["details"].get("likes"),
            "dislikes": data["details"].get("dislikes"),
            "language": data["details"].get("language"),
            "thumbnail_image_url": data.get("thumbnail_image_url")
        }
        if segment_type == PersistentSegmentType.CHANNEL:
            mapped_data["subscribers"] = data["details"].get("subscribers")
            mapped_data["audited_videos"] = data["details"].get("audited_videos")
        return mapped_data

