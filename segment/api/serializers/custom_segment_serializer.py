from django.core.files.uploadedfile import TemporaryUploadedFile
from django.db import transaction
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField
from rest_framework.exceptions import ValidationError

from audit_tool.models import AuditProcessor
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models import CustomSegmentSourceFileUpload
from segment.models.constants import CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import SourceListType
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.utils.query_builder import SegmentQueryBuilder
from userprofile.models import UserProfile


class FeaturedImageUrlMixin:
    """
    Returns a default image if not set
    """

    def get_featured_image_url(self, instance):
        return instance.featured_image_url or CUSTOM_SEGMENT_DEFAULT_IMAGE_URL

    def get_thumbnail_image_url(self, instance):
        """
        for backwards compatibility with frontend that expects this field
        """
        return self.get_featured_image_url(instance)


class CustomSegmentSerializer(FeaturedImageUrlMixin, Serializer):
    is_vetting_complete = BooleanField(read_only=True)
    is_featured = BooleanField(read_only=True)
    is_regenerating = BooleanField(read_only=True)
    owner_id = CharField(max_length=50)
    pending = SerializerMethodField()
    segment_type = CharField()
    source_name = SerializerMethodField(read_only=True)
    statistics = SerializerMethodField()
    title = CharField(max_length=255)
    title_hash = IntegerField(write_only=True)
    thumbnail_image_url = SerializerMethodField(read_only=True)

    def get_pending(self, obj):
        """
        Implicitly determine if the CTL export has been generated as a statistics value is added to the CTL row
        once an export is completed.
        """
        pending = not bool(obj.statistics)
        return pending

    def get_source_name(self, obj):
        """ Get name of uploaded source file """
        try:
            name = obj.source.name
        except CustomSegmentSourceFileUpload.DoesNotExist:
            name = None
        return name

    def get_statistics(self, obj):
        statistics = obj.statistics if obj.statistics else {
            "top_three_items": [{
                "image_url": S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL,
                "id": None,
                "title": None
            } for _ in range(3)]
        }
        return statistics

    def create(self, validated_data):
        """
        Handle CustomSegment obj creation and other required creation steps
        :param validated_data:
        :return:
        """
        try:
            with transaction.atomic():
                segment = CustomSegment.objects.create(**validated_data)
                self._create_query(segment)
                self._create_source_file(segment)
                self._start_segment_export_task(segment)
        # pylint: disable=broad-except
        except Exception as error:
            # pylint: enable=broad-except
            raise ValidationError(f"Exception trying to create segment: {error}.")
        return segment

    def validate_owner(self, owner_id):
        try:
            user = UserProfile.objects.get(id=owner_id)
        except UserProfile.DoesNotExist:
            raise ValidationError("User with id: {} not found.".format(owner_id))
        return user

    def validate_segment_type(self, segment_type):
        segment_type = int(segment_type)
        if segment_type not in (0, 1):
            raise ValidationError("segment_type must be either 0 or 1.")
        return segment_type

    def validate_title(self, title):
        hashed = self.initial_data["title_hash"]
        owner_id = self.initial_data["owner_id"]
        segment_type = self.validate_segment_type(self.initial_data["segment_type"])
        segments = CustomSegment.objects.filter(owner_id=owner_id, title_hash=hashed, segment_type=segment_type)
        if any(segment.title.lower() == title.lower().strip() for segment in segments):
            segment_type_repr = SegmentTypeEnum(segment_type).name.lower()
            raise ValidationError("A {} target list with the title: {} already exists.".format(segment_type_repr, title))
        return title

    def to_representation(self, instance):
        data = super().to_representation(instance)
        # adding this here instead of using a SerializerMethodField to preserve to-db serialization
        data["segment_type"] = SegmentTypeEnum(instance.segment_type).name.lower()
        try:
            # instance data should overwrite export query params as it is the most up-to-date
            export_query_params = instance.export.query.get("params", {})
            export_query_params.update(data)
            data = export_query_params
            data["download_url"] = instance.export.download_url
        except CustomSegmentFileUpload.DoesNotExist:
            data["download_url"] = None
        return data

    def _start_segment_export_task(self, segment):
        """
        Handle CTL export generation task
        Only execute generate_custom_segment task if CTL was created without inclusion / exclusion lists.
        CTL creation with inclusion / exclusion lists must be created through audit app first
        :param segment: CustomSegment
        :param files: dict
        :return:
        """
        extra_kwargs = {}
        files = self.context["files"]
        if files.get("inclusion_file") or files.get("exclusion_file"):
            extra_kwargs = dict(with_audit=True)
            self._create_audit(segment)
        generate_custom_segment.delay(segment.id, **extra_kwargs)

    def _create_query(self, segment):
        """
        Create Elasticsearch query body with params for export generation
        :param segment: CustomSegment
        :param validated_data: dict
        :return:
        """
        validated_ctl_params = self.context["ctl_params"]
        query_builder = SegmentQueryBuilder(validated_ctl_params)
        # Use query_builder.query_params to get mapped values used in Elasticsearch query
        query = {
            "params": query_builder.query_params,
            "body": query_builder.query_body.to_dict()
        }
        CustomSegmentFileUpload.objects.create(query=query, segment=segment)

    def _create_source_file(self, segment):
        """
        Create CTL source file using user uploaded csv
        This prepares csv for CTL export generation and is used to only include channels / videos that are both filtered
        for and on the source csv
        :param segment:
        :param validated_data:
        :return:
        """
        files = self.context["files"]
        request = self.context["request"]
        source_file = files["source_file"]
        if not source_file:
            return
        try:
            source_type = request.query_params.get("source_type", SourceListType.INCLUSION)
            source_type = SourceListType(source_type).value
        except ValueError:
            raise ValidationError(f"Invalid source_type. "
                                  f"Valid values: {SourceListType.INCLUSION.value}, {SourceListType.EXCLUSION.value}")
        key = segment.get_source_s3_key()
        segment.s3.export_object_to_s3(source_file, key)
        source_upload = CustomSegmentSourceFileUpload.objects.create(
            segment=segment,
            source_type=source_type,
            filename=key,
            name=getattr(source_file, "name", None),
        )
        return source_upload

    def _get_file_data(self, file_reader: TemporaryUploadedFile):
        """
        Util method to extract rows from inclusion / exclusion csv upload
        :param file_reader: TemporaryUploadedFile in open read +rb state
        :return:
        """
        rows = []
        for row in file_reader:
            decoded = row.decode("utf-8").lower().strip()
            if decoded:
                rows.append(decoded)
        return rows

    def _create_audit(self, segment):
        """
        Create AuditProcessor to leverage audit flow for CTL creation
        This will create an AuditProcessor that will handle detecting inclusion / exclusion words that were uploaded
            during CTL creation
        After generate_custom_segment task is finished creating export with CTL filters, the process will upload the
            csv export for audit processes to filter again using inclusion / exclusion words to upload a final csv
            export for the list
        :param segment:
        :param files:
        :return:
        """
        files = self.context["files"]
        inclusion_file = files.get("inclusion_file")
        exclusion_file = files.get("exclusion_file")
        inclusion_rows = self._get_file_data(inclusion_file) if inclusion_file else []
        exclusion_rows = self._get_file_data(exclusion_file) if exclusion_file else []
        params = dict(
            user_id=self.context["request"].user.id,
            inclusion_hit_count=1,
            exclusion_hit_count=1,
            exclusion=exclusion_rows,
            inclusion=inclusion_rows,
            segment_id=segment.id,
            source=2,
        )
        if segment.segment_type == 0:
            # Video config
            extra_data = dict(
                audit_type=1
            )
        else:
            # Channel config
            extra_data = dict(
                do_videos=False,
                num_videos=0,
                audit_type=2,
            )
        params.update(extra_data)
        # Audit is initially created with temp_stop=True to prevent from processing immediately. Audit will be updated
        # to temp_stop=False once generate_custom_segment completes with finished source file for audit
        audit = AuditProcessor.objects.create(temp_stop=True, params=params)
        return audit


class CustomSegmentWithoutDownloadUrlSerializer(CustomSegmentSerializer):
    def to_representation(self, instance):
        """
        overrides CustomSegmentSerializer. Users without certain permissions
        shouldn't be able to see download_url
        """
        data = super().to_representation(instance)
        data.pop("download_url", None)
        return data
