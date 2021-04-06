import csv
import io
import logging
import os
import tempfile

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.core.files.uploadedfile import TemporaryUploadedFile
from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import Field
from rest_framework.serializers import IntegerField
from rest_framework.serializers import JSONField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from audit_tool.api.views import AuditSaveApiView
from audit_tool.models import AuditProcessor
from audit_tool.models import get_hash_name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models import CustomSegmentSourceFileUpload
from segment.models.constants import CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import SourceListType
from segment.models.constants import Params
from segment.models.constants import Results
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.tasks.generate_video_exclusion import generate_video_exclusion
from segment.utils.query_builder import SegmentQueryBuilder
from segment.utils.utils import delete_related
from userprofile.models import UserProfile
from utils.aws.s3_exporter import ReportNotFoundException
from utils.datetime import seconds_to_hhmmss
from utils.utils import validate_youtube_url

logger = logging.getLogger(__name__)


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


class SegmentTypeField(Field):
    def to_internal_value(self, data):
        try:
            SegmentTypeEnum(data)
        except ValueError:
            raise ValidationError(f"Invalid segment_type: {data}")
        return data

    def to_representation(self, value):
        segment_type = SegmentTypeEnum(value).name.lower()
        return segment_type


class CTLSerializer(FeaturedImageUrlMixin, Serializer):
    """
    Serializer to handle creating and updating CustomSegments
    During both creates / updates, the view request object, CTLParamsSerializer.validated_data, and files dict
    must be passed as context for successful validation
    """
    SOURCE_LIST_MAX_SIZE = 200000

    audit_id = IntegerField(allow_null=True, read_only=True)
    ctl_params = SerializerMethodField()
    id = IntegerField(required=False)
    is_featured = BooleanField(read_only=True)
    is_vetting_complete = BooleanField(read_only=True)
    is_regenerating = BooleanField(read_only=True)
    last_vetted_date = SerializerMethodField()
    owner_id = CharField(read_only=True)
    params = JSONField(read_only=True)
    pending = SerializerMethodField()
    segment_type = SegmentTypeField()
    source_name = SerializerMethodField(read_only=True)
    statistics = SerializerMethodField()
    title = CharField(max_length=255)
    thumbnail_image_url = SerializerMethodField(read_only=True)
    created_at = DateTimeField(read_only=True)
    updated_at = DateTimeField(read_only=True)

    def get_ctl_params(self, obj: CustomSegment) -> dict:
        """
        Serialize params that were used to create CTL that is stored in
            related CustomSegmentFileUpload.query["params"]
        """
        try:
            ctl_params = obj.export.query.get("params", {})
            # convert seconds to HH:MM:SS format for display
            minimum_duration = ctl_params.get("minimum_duration", None)
            if minimum_duration is not None:
                ctl_params["minimum_duration"] = seconds_to_hhmmss(minimum_duration)
            maximum_duration = ctl_params.get("maximum_duration", None)
            if maximum_duration is not None:
                ctl_params["maximum_duration"] = seconds_to_hhmmss(maximum_duration)
        except CustomSegmentFileUpload.DoesNotExist:
            ctl_params = {}
        return ctl_params

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

    def get_statistics(self, obj: CustomSegment) -> dict:
        statistics = obj.statistics
        if obj.segment_type == SegmentTypeEnum.CHANNEL.value:
            video_exclusion_filename = statistics.get(Results.VIDEO_EXCLUSION_FILENAME, False)
            # If params set but filename is unavailable, video exclusion ctl is being generated so
            # serialize as None to represent "pending"
            if obj.params.get(Params.WITH_VIDEO_EXCLUSION) is True \
                    and not video_exclusion_filename:
                statistics[Results.VIDEO_EXCLUSION_FILENAME] = None
            else:
                # Simply serialize with result of get call. If filename was set, then it is available for export.
                # If not, then video_exclusion_filename will be False, which represents it is available for creation.
                statistics[Results.VIDEO_EXCLUSION_FILENAME] = video_exclusion_filename
        return statistics

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
        # owner is instance owner if PATCH, or requesting user if POST
        owner_id = getattr(self.instance, "owner_id", self.context["request"].user.id)
        segment_type = self.validate_segment_type(self.initial_data["segment_type"])
        segments = CustomSegment.objects.filter(owner_id=owner_id, title_hash=hashed, segment_type=segment_type)
        if isinstance(self.instance, CustomSegment):
            segments = segments.exclude(id=self.instance.id)
        if any(segment.title.lower() == title.lower().strip() for segment in segments):
            segment_type_repr = SegmentTypeEnum(segment_type).name.lower()
            raise ValidationError(f"A {segment_type_repr} target list with the title: {title} already exists.")
        return title

    def create(self, validated_data: dict) -> CustomSegment:
        """
        Handle CustomSegment obj creation
        :param validated_data: dict
        :return: CustomSegment
        """
        title_hash = get_hash_name(validated_data["title"].lower().strip())
        segment = CustomSegment.objects.create(owner_id=self.context["request"].user.id, title_hash=title_hash,
                                               **validated_data)
        try:
            self._create_export(segment)
            # pylint: disable=broad-except
        except ValidationError as err:
            segment.statistics["error"] = str(err.detail[0])
            segment.save(update_fields=["statistics"])
            raise err
        except CTLEmptySourceUrlException:
            segment_type_repr = SegmentTypeEnum(segment.segment_type).name
            delete_related(segment)
            raise ValidationError(f"Mismatching file format. {segment_type_repr.capitalize()} lists "
                                  f"need {segment_type_repr.lower()} urls. Please adjust and try again")
        except Exception as error:
            # pylint: enable=broad-except
            # Delete CTL if unexpected exception occurs
            delete_related(segment)
            raise ValidationError(f"Exception trying to create segment: {error}. Please try again.")
        return segment

    def update(self, instance, validated_data: dict) -> CustomSegment:
        """
        Update CustomSegment
        If any ctl params or files inclusion/exclusion keywords have changed, regenerate the export with updated params
        :param instance: CustomSegment
        :param validated_data: dict
        :return:
        """
        # Check only creating video exclusion ctl
        video_exclusion_params = self.context.get("video_exclusion_params")
        if video_exclusion_params.get(Params.WITH_VIDEO_EXCLUSION) is True:
            instance.params.update(video_exclusion_params)
            instance.save(update_fields=["params"])
            generate_video_exclusion.delay(instance.id)
            return instance
        try:
            old_params = instance.export.query.get("params", {})
        except CustomSegmentFileUpload.DoesNotExist:
            old_params = {}
        new_params = self.context["ctl_params"]
        should_regenerate = self._check_should_regenerate(instance, old_params, new_params)
        old_meta_audit_id = instance.params.get(Params.META_AUDIT_ID)
        # always save updated title
        title = validated_data.get("title", instance.title)
        if title != instance.title:
            instance.title = title
            instance.title_hash = get_hash_name(title)
            instance.save(update_fields=["title", "title_hash", "updated_at"])
        if should_regenerate:
            # Consider regeneration as new list and delete associated records used with old generated export
            self._clean_ctl(instance)
            self._create_export(instance)
            updated_params = {"stopped": True}
            updated_attrs = {"completed": timezone.now(), "pause": 0}
        else:
            updated_params = {"name": title}
            updated_attrs = {"name": title.lower()}
        try:
            # If regenerating, update audit to pause for new audit to process. Else, update name with segment name
            audit = AuditProcessor.objects.get(id=old_meta_audit_id)
            [setattr(audit, key, value) for key, value in updated_attrs.items()]
            audit.params.update(updated_params)
            audit.save()
        except AuditProcessor.DoesNotExist:
            pass
        return instance

    def _check_should_regenerate(self, segment: CustomSegment, old_params: dict, new_params: dict) -> bool:
        """
        Check params and files for changes to determine if export file should be regenerated during updates
        Changes for files should only be checked if a file is sent in the request. For example if a CTL was created
            with a source list but subsequent edits to the CTL do not include a source_file in the request, do not
            check for changes for the file
        :param segment: CustomSegment
        :param old_params: dict -> Old ctl_params dict saved on related CustomSegmentFileUpload
        :param new_params: dict -> New ctl_params dict from request body
        :return:
        """
        should_regenerate = False
        files = self.context["files"]
        source_file = files.get("source_file")

        # Check ctl filters for changes. Check each key as may be updating with partial dict of original params
        matching_old_params = {key: old_params.get(key) for key in new_params.keys()}
        if new_params != matching_old_params:
            should_regenerate = True
            return should_regenerate

        # Check source file for changes. Only filter for valid ids returned from validate_youtube_url
        if source_file is not None:
            try:
                old_ids = list(filter(lambda _id: _id, [validate_youtube_url(url, segment.segment_type, default="").upper()
                                                   for url in segment.s3.get_extract_export_ids(segment.source.filename)]))
            except (CustomSegmentSourceFileUpload.DoesNotExist, ReportNotFoundException):
                old_ids = []
            try:
                new_ids = list(filter(lambda _id: _id, [validate_youtube_url(url, segment.segment_type, default="").upper()
                                                   for url in self._get_source_file_data(source_file)]))
            except TypeError:
                new_ids = []
            if set(old_ids) != set(new_ids):
                should_regenerate = True
                return should_regenerate
        try:
            audit = AuditProcessor.objects.get(id=segment.params[Params.META_AUDIT_ID])

            # Check inclusion / exclusion keywords
            inclusion_filename, inclusion_rows = self._get_inclusion_keywords(audit.params)
            exclusion_filename, exclusion_data, _ = self._get_exclusion_keywords(audit.params)

            # Check if updating with new files with changes
            inclusion_changed = inclusion_filename is not None and set(inclusion_rows) != set(audit.params.get("inclusion", {}))
            exclusion_changed = exclusion_filename is not None and self._check_exclusion_changed(audit.params.get("exclusion", []), exclusion_data)
            # Check if files were removed. If existing audit has keywords but the request sends thresholds as None, then
            # it is considered removed and should_regenerate
            inclusion_file_removed = audit.params.get("inclusion") is not None and new_params["inclusion_hit_threshold"] is None
            exclusion_file_removed = audit.params.get("exclusion") is not None and new_params["inclusion_hit_threshold"] is None
            if inclusion_changed or exclusion_changed or inclusion_file_removed or exclusion_file_removed:
                should_regenerate = True
        except (KeyError, AuditProcessor.DoesNotExist):
            pass
        return should_regenerate

    def _check_exclusion_changed(self, old_audit_exclusion_data, new_exclusion_data):
        old_rows = set([",".join(row) for row in old_audit_exclusion_data])
        new_rows = set([",".join(row) for row in new_exclusion_data])
        return old_rows != new_rows

    def _create_export(self, segment: CustomSegment) -> CustomSegment:
        """
        Method to handle invoking necessary methods for full CTL creation
        These methods are used here because both create or update methods may call this method
        """
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
        if files.get(Params.INCLUSION_FILE) or files.get(Params.EXCLUSION_FILE) \
                or AuditProcessor.objects.filter(id=segment.params.get(Params.META_AUDIT_ID)).exists():
            audit = self._create_audit(segment)
            # If an audit was created, then create CTL with audit. Audits however will not always be created. For
            # example, if updating a CTl that has inclusion / exclusion keywords but is being removed during the update,
            # then there is no need to create an audit
            if audit:
                extra_kwargs = dict(with_audit=True)
        generate_custom_segment.delay(segment.id, **extra_kwargs)

    def _create_query(self, segment: CustomSegment) -> None:
        """
        Create or update Elasticsearch query body with params for export generation
        This method may be used for both creating and partially updating params. First get or create
            CustomSegmentSourceFileUpload to update its query["body"] key and then create full query["body"] for
            Elasticsearch query during export generation
        :param segment: CustomSegment
        :return:
        """
        validated_ctl_params = self.context["ctl_params"]
        # Default with empty query for creation
        query = dict(params={}, body={})
        related_file, _ = CustomSegmentFileUpload.objects\
            .get_or_create(segment=segment, defaults=dict(query=query, segment=segment))
        # Update params dict as validated_ctl_params may be a partial dict of full CTL params
        related_file.query["params"].update(validated_ctl_params)
        es_query_body = SegmentQueryBuilder(related_file.query["params"]).query_body.to_dict()
        related_file.query["body"] = es_query_body
        related_file.save(update_fields=["query"])

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

        final_source_file = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
        try:
            rows = []
            with io.TextIOWrapper(source_file, encoding="utf-8") as source_text,\
                    open(final_source_file, mode="w") as dest:
                reader = csv.reader(source_text, delimiter=",")
                for row in reader:
                    try:
                        valid_url = validate_youtube_url(row[0], segment.segment_type)
                    except IndexError:
                        # Catch empty rows at end of csv
                        continue
                    if valid_url:
                        rows.append(row)
                    if len(rows) >= self.SOURCE_LIST_MAX_SIZE:
                        break
                if not rows:
                    raise CTLEmptySourceUrlException
                writer = csv.writer(dest)
                writer.writerows(rows)

            key = segment.get_source_s3_key()
            segment.s3.export_file_to_s3(final_source_file, key)
            source_upload, _ = CustomSegmentSourceFileUpload.objects.update_or_create(
                segment=segment,
                defaults=dict(
                    source_type=source_type,
                    filename=key,
                    name=getattr(source_file, "name", None)
                )
            )
        except CTLEmptySourceUrlException:
            raise
        except Exception:
            message = "Error creating CTL source file"
            logger.exception(message)
            raise ValidationError(message)
        finally:
            os.remove(final_source_file)

    def _create_audit(self, segment: CustomSegment) -> AuditProcessor:
        """
        Create AuditProcessor to leverage audit flow for CTL creation
        This will create an AuditProcessor that will handle detecting inclusion / exclusion words that were uploaded
            during CTL creation
        After generate_custom_segment task is finished creating export with CTL filters, the process will upload the
            csv export for audit processes to filter again using inclusion / exclusion words to upload a final csv
            export for the list in the audit_tool.management.commands.audit_video_meta.Command.update_ctl method
        """
        try:
            old_params = AuditProcessor.objects.get(id=segment.params.get(Params.META_AUDIT_ID)).params
        except AuditProcessor.DoesNotExist:
            old_params = {}
        request = self.context["request"]
        inclusion_filename, inclusion_rows = self._get_inclusion_keywords(old_params)
        exclusion_filename, exclusion_rows, exclusion_categories = self._get_exclusion_keywords(old_params)

        # If a CTL was created with keywords and both are being removed during the update, then there is no need to
        # create an audit without keywords. Remove audit metadata from segment params
        if old_params.get("inclusion") and not inclusion_rows and old_params.get("exclusion") and not exclusion_rows:
            segment.remove_meta_audit_params()
            return

        params = dict(
            name=segment.title,
            segment_id=segment.id,
            user_id=request.user.id,
            inclusion=inclusion_rows,
            exclusion=exclusion_rows,
            exclusion_category=exclusion_categories,
            inclusion_hit_count=self.context["ctl_params"]["inclusion_hit_threshold"] or 1,
            exclusion_hit_count=self.context["ctl_params"]["exclusion_hit_threshold"] or 1,
            files={
                "inclusion": inclusion_filename,
                "exclusion": exclusion_filename,
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
                do_videos=True,
                num_videos=15,
                audit_type_original=audit_type
            )
        params.update(extra_params)
        # Audit is initially created with temp_stop=True to prevent from processing immediately. Audit will be updated
        # to temp_stop=False once generate_custom_segment completes with finished source file for audit. The audit
        # update is done in the segment.models.utils.generate_segment_utils.GenerateSegmentUtils.start_audit method
        audit = AuditProcessor.objects.create(source=2, audit_type=audit_type, temp_stop=True,
                                              name=segment.title.lower(), params=params)
        segment.params.update({
            Params.INCLUSION_FILE: inclusion_filename,
            Params.EXCLUSION_FILE: exclusion_filename,
            Params.META_AUDIT_ID: audit.id,
        })
        segment.save(update_fields=["params"])
        return audit

    def _get_inclusion_keywords(self, old_audit_params):
        """ Helper method to get inclusion data as return value for _get_keyword_file_data changes if exclusion """
        filename, file_data = self._get_keyword_file_data(old_audit_params, keyword_type="inclusion")
        return filename, file_data

    def _get_exclusion_keywords(self, old_audit_params):
        """ Helper method to get exclusion data as return value for _get_keyword_file_data changes if exclusion """
        filename, file_data = self._get_keyword_file_data(old_audit_params, keyword_type="exclusion")
        # If exclusion file provided, exclusion_data is tuple result from
        # AuditSaveApiView.load_exclusion_keywords method
        try:
            exclusion_rows = file_data[0]
            exclusion_categories = file_data[1]
        except IndexError:
            exclusion_rows = exclusion_categories = []
        return filename, exclusion_rows, exclusion_categories

    def _get_keyword_file_data(self, old_audit_params, keyword_type="inclusion"):
        """
        Method to help determine filename and file data
        Raises ValidationError if keyword file is empty
        :param old_audit_params: dict -> Old meta audit params
        :param keyword_type: inclusion | exclusion
        :return: tuple
        """
        if keyword_type == "inclusion":
            keyword_extractor = AuditSaveApiView().load_keywords
        else:
            keyword_extractor = AuditSaveApiView().load_exclusion_keywords
        files = self.context["files"]
        file = files.get(f"{keyword_type}_file")

        # If hit_threshold is None but file data was provided before, then implicitly removing file
        # e.g. CTL was created with inclusion_file and hit_threshold of 1 and updating with hit_threshold = None, then
        # remove the file
        hit_threshold = self.context["ctl_params"][f"{keyword_type}_hit_threshold"]
        # Creating brand new audit or updating existing keywords, use provided file data
        if (not old_audit_params and file is not None) or (old_audit_params.get(keyword_type) and file):
            filename = file.name
            file_data = keyword_extractor(file)
            if len(file_data) <= 0 or len(file_data[0]) <= 0:
                raise ValidationError(f"Error: empty {keyword_type} keywords file")
        # Creating brand new audit with no file or removing the file sets empty data
        elif (not file and not old_audit_params) or (old_audit_params.get(keyword_type) and hit_threshold is None):
            filename = None
            file_data = []
        # Not modifying the file, use old audit params if possible
        else:
            filename = old_audit_params["files"].get(keyword_type)
            if keyword_type == "inclusion":
                file_data = old_audit_params.get(keyword_type)
            else:
                # Exclusion data should be tuple as AuditSaveApiView.load_exclusion_keywords returns tuple
                file_data = (old_audit_params.get(keyword_type), old_audit_params.get("exclusion_categories"))
        # Reset file for other processes
        try:
            file.seek(0)
        except AttributeError:
            pass
        return filename, file_data

    def _get_source_file_data(self, file_reader: TemporaryUploadedFile) -> list:
        with open(file_reader.file.name, mode="r") as file:
            reader = csv.reader(file, delimiter=",")
            rows = [row[0].strip() for row in reader]
        return rows

    def _get_file_data(self, file_reader: TemporaryUploadedFile) -> list:
        """
        Util method to extract rows from inclusion / exclusion csv upload
        :param file_reader: TemporaryUploadedFile in open read +rb state
        :return: list
        """
        with open(file_reader.file.name, mode="r") as file:
            reader = csv.reader(file, delimiter=",")
            rows = [row[0].strip() for row in reader]
        return rows

    def _clean_ctl(self, segment: CustomSegment):
        """ Util method for update method to help delete associated records with old CTL export """
        set_false = ["is_vetting_complete", "is_featured", "is_regenerating"]
        # Delete audit used for vetting
        AuditProcessor.objects.filter(id__in=[segment.audit_id]).delete()
        [setattr(segment, key, False) for key in set_false]
        segment.audit_id = None
        segment.statistics = {}
        segment.params[Params.WITH_VIDEO_EXCLUSION] = False
        if hasattr(segment, "export"):
            segment.export.delete()
        if hasattr(segment, "vetted_export"):
            segment.vetted_export.delete()
        segment.save(update_fields=[*set_false, "audit_id", "statistics", "params"])


class CTLWithoutDownloadUrlSerializer(CTLSerializer):
    def to_representation(self, instance):
        """
        overrides CustomSegmentSerializer. Users without certain permissions
        shouldn't be able to see download_url
        """
        data = super().to_representation(instance)
        data.pop("download_url", None)
        return data


class CTLEmptySourceUrlException(Exception):
    pass
