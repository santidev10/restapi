import datetime

from django.core.exceptions import ObjectDoesNotExist
from django.core.files.uploadedfile import TemporaryUploadedFile
from django.db import transaction
from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import JSONField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from audit_tool.models import AuditProcessor
from audit_tool.models import get_hash_name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models import CustomSegmentSourceFileUpload
from segment.models.constants import CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import SourceListType
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.utils.query_builder import SegmentQueryBuilder
from userprofile.models import UserProfile
from utils.aws.s3_exporter import ReportNotFoundException


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


class CTLSerializer(FeaturedImageUrlMixin, Serializer):
    audit_id = IntegerField(allow_null=True, read_only=True)
    id = IntegerField(required=False)
    is_featured = BooleanField(read_only=True)
    is_vetting_complete = BooleanField(read_only=True)
    is_regenerating = BooleanField(read_only=True)
    last_vetted_date = SerializerMethodField()
    owner_id = CharField(read_only=True)
    params = JSONField(read_only=True)
    pending = SerializerMethodField()
    segment_type = CharField()
    source_name = SerializerMethodField(read_only=True)
    statistics = JSONField(read_only=True)
    title = CharField(max_length=255)
    thumbnail_image_url = SerializerMethodField(read_only=True)

    def get_last_vetted_date(self, obj: CustomSegment) -> str:
        """
        Get date the CTL was last vetted if vetting is enabled
        """
        try:
            audit = AuditProcessor.objects.get(id=obj.audit_id)
            last_vetted_date = obj.audit_utils.vetting_model.objects\
                .filter(audit=audit, processed__isnull=False)\
                .latest("processed").processed.date().strftime("%m/%d/%Y")
        except ObjectDoesNotExist:
            last_vetted_date = None
        return last_vetted_date

    def get_pending(self, obj: CustomSegment) -> bool:
        """
        Implicitly determine if the CTL export has been generated as a statistics value is added to the CTL row
        once an export is completed.
        """
        pending = not bool(obj.statistics)
        return pending

    def get_source_name(self, obj: CustomSegment) -> str:
        """ Get name of uploaded source file """
        try:
            name = obj.source.name
        except CustomSegmentSourceFileUpload.DoesNotExist:
            name = None
        return name

    def validate_owner(self, owner_id: int) -> UserProfile:
        try:
            user = UserProfile.objects.get(id=owner_id)
        except UserProfile.DoesNotExist:
            raise ValidationError("User with id: {} not found.".format(owner_id))
        return user

    def validate_segment_type(self, segment_type: int) -> int:
        segment_type = int(segment_type)
        if segment_type not in (0, 1):
            raise ValidationError("segment_type must be either 0 or 1.")
        return segment_type

    def validate_title(self, title: str) -> str:
        hashed = get_hash_name(title.lower().strip())
        owner_id = self.context["request"].user.id
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
            # convert seconds to HH:MM:SS format for display
            minimum_duration = data.get("minimum_duration", None)
            if minimum_duration:
                data["minimum_duration"] = str(datetime.timedelta(seconds=minimum_duration))
            maximum_duration = data.get("maximum_duration", None)
            if maximum_duration:
                data["maximum_duration"] = str(datetime.timedelta(seconds=maximum_duration))
        except CustomSegmentFileUpload.DoesNotExist:
            pass
        return data

    def create(self, validated_data: dict) -> CustomSegment:
        """
        Handle CustomSegment obj creation
        :param validated_data: dict
        :return: CustomSegment
        """
        try:
            title_hash = get_hash_name(validated_data["title"].lower().strip())
            segment = CustomSegment.objects.create(owner_id=self.context["request"].user.id, title_hash=title_hash,
                                                   **validated_data)
            self._create_export(segment)
            # pylint: disable=broad-except
        except Exception as error:
            # pylint: enable=broad-except
            raise ValidationError(f"Exception trying to create segment: {error}.")
        return segment

    def update(self, instance, validated_data: dict) -> CustomSegment:
        """
        Update CustomSegment
        If any ctl params or files inclusion/exclusion keywords have changed, regenerate the export with updated params
        :param instance: CustomSegment
        :param validated_data: dict
        :return:
        """
        try:
            old_params = instance.export.query.get("params", {})
        except CustomSegmentFileUpload.DoesNotExist:
            old_params = {}
        new_params = self.context["ctl_params"]
        should_regenerate = self._check_should_regenerate(instance, old_params, new_params)
        old_audit_id = instance.params.get("meta_audit_id")
        if should_regenerate:
            self._create_export(instance)
            updated_params = {"stopped": True}
            updated_attrs = {"completed": timezone.now(), "pause": 0}
        else:
            updated_params = {"name": instance.title}
            updated_attrs = {"name": instance.title.lower()}
        try:
            # If regenerating, update audit to pause for new audit to process. Else, update name with segment name
            audit = AuditProcessor.objects.get(id=old_audit_id)
            [setattr(audit, key, value) for key, value in updated_attrs.items()]
            audit.params.update(updated_params)
            audit.save()
        except AuditProcessor.DoesNotExist:
            pass
        return instance

    def _check_should_regenerate(self, segment: CustomSegment, old_params: dict, new_params: dict) -> bool:
        """
        Check params and files for changes to determine if export file should be regenerated
        :param segment:
        :param old_params: dict -> Old ctl_params dict saved on related CustomSegmentFileUpload
        :param new_params: dict -> New ctl_params dict from request body
        :return:
        """
        should_regenerate = False
        files = self.context["files"]
        source_file = files.get("source_file")
        inclusion_file = files.get("inclusion_file")
        exclusion_file = files.get("exclusion_file")

        # Check ctl filters
        if new_params != old_params:
            should_regenerate = True
            return should_regenerate

        # Check source file. Source file must be checked even if one was not sent in the current request in the case of
        # a source file being uploaded before and now without one, effectively changing the CTL to have no source file
        try:
            old_ids = [segment.s3.parse_url(url, item_type=segment.segment_type).upper()
                       for url in segment.s3.get_extract_export_ids(segment.source.filename)]
        except (CustomSegmentSourceFileUpload.DoesNotExist, ReportNotFoundException):
            old_ids = []
        try:
            new_ids = [segment.s3.parse_url(url, item_type=segment.segment_type).upper()
                       for url in self._get_file_data(source_file)]
        except TypeError:
            new_ids = []
        if set(old_ids) != set(new_ids):
            should_regenerate = True
            return should_regenerate

        # Check inclusion / exclusion keywords
        inclusion_rows = self._get_file_data(inclusion_file) if inclusion_file else []
        exclusion_rows = self._get_file_data(exclusion_file) if exclusion_file else []
        try:
            audit = AuditProcessor.objects.get(id=segment.params["meta_audit_id"])
            if set(inclusion_rows) != set(audit.params.get("inclusion", {})) \
                    or set(exclusion_rows) != set(audit.params.get("exclusion", {})):
                should_regenerate = True
        except (KeyError, AuditProcessor.DoesNotExist):
            pass
        return should_regenerate

    def _create_export(self, segment: CustomSegment) -> CustomSegment:
        """
        Method to handle invoking necessary methods for full CTL creation
        These methods are used here because both create or update methods may call this method
        """
        with transaction.atomic():
            self._create_query(segment)
            self._create_source_file(segment)
            self._start_segment_export_task(segment)
        return segment

    def _start_segment_export_task(self, segment: CustomSegment):
        """
        Handle CTL export generation task
        If CTL is created with inclusion / exclusion keyword lists, then ctl will be further processed by audit
        """
        extra_kwargs = {}
        files = self.context["files"]
        if files.get("inclusion_file") or files.get("exclusion_file"):
            extra_kwargs = dict(with_audit=True)
            self._create_audit(segment)
        generate_custom_segment.delay(segment.id, **extra_kwargs)

    def _create_query(self, segment: CustomSegment):
        """
        Create Elasticsearch query body with params for export generation
        :param segment: CustomSegment
        :return:
        """
        validated_ctl_params = self.context["ctl_params"]
        query_builder = SegmentQueryBuilder(validated_ctl_params)
        query = {
            "params": validated_ctl_params,
            "body": query_builder.query_body.to_dict()
        }
        CustomSegmentFileUpload.objects.update_or_create(segment=segment, defaults=dict(query=query, segment=segment))

    def _create_source_file(self, segment: CustomSegment) -> CustomSegmentSourceFileUpload:
        """
        Create CTL source file using user uploaded csv
        This prepares csv for CTL export generation and is used to only include channels / videos that are both filtered
        for and on the source csv
        """
        files = self.context["files"]
        request = self.context["request"]
        source_file = files.get("source_file")
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
        source_upload, _ = CustomSegmentSourceFileUpload.objects.update_or_create(
            segment=segment,
            defaults=dict(
                source_type=source_type,
                filename=key,
                name=getattr(source_file, "name", None)
            )
        )
        return source_upload

    def _create_audit(self, segment: CustomSegment) -> AuditProcessor:
        """
        Create AuditProcessor to leverage audit flow for CTL creation
        This will create an AuditProcessor that will handle detecting inclusion / exclusion words that were uploaded
            during CTL creation
        After generate_custom_segment task is finished creating export with CTL filters, the process will upload the
            csv export for audit processes to filter again using inclusion / exclusion words to upload a final csv
            export for the list
        """
        files = self.context["files"]
        request = self.context["request"]
        inclusion_file = files.get("inclusion_file")
        exclusion_file = files.get("exclusion_file")
        inclusion_rows = self._get_file_data(inclusion_file) if inclusion_file else []
        exclusion_rows = self._get_file_data(exclusion_file) if exclusion_file else []
        params = dict(
            source=2,
            name=segment.title,
            segment_id=segment.id,
            user_id=request.user.id,
            inclusion=inclusion_rows,
            exclusion=exclusion_rows,
            inclusion_hit_count=self.context["ctl_params"]["inclusion_hit_threshold"] or 1,
            exclusion_hit_count=self.context["ctl_params"]["exclusion_hit_threshold"] or 1,
            files={
                "inclusion": getattr(inclusion_file, "name", None),
                "exclusion": getattr(exclusion_file, "name", None),
            }
        )
        if segment.segment_type == 0:
            # Video config
            audit_type = 1
            extra_params = dict(
                audit_type_original=audit_type
            )
        else:
            # Channel config
            audit_type = 2
            extra_params = dict(
                do_videos=False,
                num_videos=0,
                audit_type_original=audit_type
            )
        params.update(extra_params)
        # Audit is initially created with temp_stop=True to prevent from processing immediately. Audit will be updated
        # to temp_stop=False once generate_custom_segment completes with finished source file for audit
        audit = AuditProcessor.objects.create(audit_type=audit_type, temp_stop=True, name=segment.title.lower(),
                                              params=params)
        segment.params.update({
            "inclusion_file": getattr(inclusion_file, "name", None),
            "exclusion_file": getattr(exclusion_file, "name", None),
            "meta_audit_id": audit.id,
        })
        segment.save()
        return audit

    def _get_file_data(self, file_reader: TemporaryUploadedFile) -> list:
        """
        Util method to extract rows from inclusion / exclusion csv upload
        :param file_reader: TemporaryUploadedFile in open read +rb state
        :return: list
        """
        rows = []
        for row in file_reader:
            decoded = row.decode("utf-8").lower().strip()
            if decoded:
                rows.append(decoded)
        file_reader.seek(0)
        return rows


class CustomSegmentWithoutDownloadUrlSerializer(CTLSerializer):
    def to_representation(self, instance):
        """
        overrides CustomSegmentSerializer. Users without certain permissions
        shouldn't be able to see download_url
        """
        data = super().to_representation(instance)
        data.pop("download_url", None)
        return data
