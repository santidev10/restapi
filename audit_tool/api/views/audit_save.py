import csv
import json
from datetime import datetime
from distutils.util import strtobool
from io import StringIO
from uuid import uuid4

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.api.serializers.audit_processor_serializer import AuditProcessorSerializer
from audit_tool.models import AuditProcessor
from audit_tool.tasks.generate_audit_items import generate_audit_items
from brand_safety.languages import LANGUAGES
from segment.models import CustomSegment
from userprofile.constants import StaticPermissions
from utils.aws.s3_exporter import S3Exporter


class AuditSaveApiView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.AUDIT_QUEUE__CREATE, method="post")
        | StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_VET_ENABLE, method="patch"),

    )
    LANGUAGES_REVERSE = {}

    def post(self, request):
        query_params = request.query_params
        user_id = request.user.id
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        do_videos = strtobool(query_params["do_videos"]) if "do_videos" in query_params else None
        move_to_top = strtobool(query_params["move_to_top"]) if "move_to_top" in query_params else None
        name = query_params["name"] if "name" in query_params else None
        audit_type = int(query_params["audit_type"]) if "audit_type" in query_params else None
        category = query_params["category"] if "category" in query_params else None
        related_audits = query_params["related_audits"] if "related_audits" in query_params else None
        source_file = request.data["source_file"] if "source_file" in request.data else None
        exclusion_file = request.data["exclusion_file"] if "exclusion_file" in request.data else None
        inclusion_file = request.data["inclusion_file"] if "inclusion_file" in request.data else None
        min_likes = int(query_params["min_likes"]) if "min_likes" in query_params else None
        min_views = int(query_params["min_views"]) if "min_views" in query_params else None
        max_dislikes = int(query_params["max_dislikes"]) if "max_dislikes" in query_params else None
        min_date = query_params["min_date"] if "min_date" in query_params else None
        num_videos = int(query_params["num_videos"]) if "num_videos" in query_params else None
        max_recommended_type = query_params[
            "max_recommended_type"] if "max_recommended_type" in query_params else "video"
        exclusion_hit_count = query_params["exclusion_hit_count"] if "exclusion_hit_count" in query_params else 1
        inclusion_hit_count = query_params["inclusion_hit_count"] if "inclusion_hit_count" in query_params else 1
        include_unknown_views = strtobool(
            query_params["include_unknown_views"]) if "include_unknown_views" in query_params else False
        include_unknown_likes = strtobool(
            query_params["include_unknown_likes"]) if "include_unknown_likes" in query_params else False
        force_data_refresh = strtobool(query_params["force_data_refresh"]) if "force_data_refresh" in query_params else None
        override_blocklist = strtobool(
            query_params["override_blocklist"]) if "override_blocklist" in query_params else None

        if not audit_id and not name:
            raise ValidationError("name can not be empty for a new audit")
        if name:
            name = name.strip()
        if min_date:
            if "/" not in min_date:
                raise ValidationError("format of min_date must be mm/dd/YYYY")
            v_date = min_date.split("/")
            if len(v_date) < 3:
                raise ValidationError("format of min_date must be mm/dd/YYYY")
            m = int(v_date[0])
            d = int(v_date[1])
            y = int(v_date[2])
            if d < 1 or d > 31:
                raise ValidationError("day must be between 1 and 31")
            if m < 1 or m > 12:
                raise ValidationError("month must be between 1 and 12")
            if y < 2000 or y > datetime.now().year:
                raise ValidationError("year must be between 2000 and {}".format(datetime.now().year))
        if move_to_top and audit_id:
            try:
                audit = AuditProcessor.objects.get(source=0, id=audit_id)
                lowest_priority = \
                AuditProcessor.objects.filter(source=0, completed__isnull=True).exclude(id=audit_id).order_by("pause")[0]
                audit.pause = lowest_priority.pause - 1
                audit.save(update_fields=["pause"])
                return Response(audit.to_dict())
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                raise ValidationError("invalid request")
        try:
            max_recommended = int(query_params["max_recommended"]) if "max_recommended" in query_params else 100000
            if max_recommended > 500000:
                max_recommended = 500000
        except ValueError:
            raise ValidationError("Expected max_recommended ({}) to be <int> type object. Received object of type {}."
                                  .format(query_params["max_recommended"], type(query_params["max_recommended"])))
        language = query_params["language"] if "language" in query_params else None

        # Audit Name Validation
        if not audit_id and name is None:
            raise ValidationError("Name field is required.")
        if name and len(name) < 3:
            raise ValidationError("Name {} must be at least 3 characters long.".format(name))
        # Audit Type Validation
        if not audit_id and audit_type is None:
            raise ValidationError("Audit_type field is required.")
        if not audit_id and str(audit_type) not in AuditProcessor.AUDIT_TYPES:
            raise ValidationError("Expected Audit Type to have one of the following values: {}. Received {}.".format(
                AuditProcessor.AUDIT_TYPES, audit_type
            ))
        # Source File Validation
        params = {
            "name": name,
            "language": language,
            "user_id": user_id,
            "do_videos": do_videos,
            "category": category,
            "related_audits": related_audits,
            "audit_type_original": audit_type,
            "min_likes": min_likes,
            "min_date": min_date,
            "min_views": min_views,
            "max_dislikes": max_dislikes,
            "num_videos": num_videos,
            "override_blocklist": override_blocklist,
            "max_recommended_type": max_recommended_type,
            "files": {
                "inclusion": None,
                "exclusion": None,
                "source": None
            },
            "exclusion_hit_count": exclusion_hit_count,
            "inclusion_hit_count": inclusion_hit_count,
            "include_unknown_views": include_unknown_views,
            "include_unknown_likes": include_unknown_likes,
            "force_data_refresh": force_data_refresh,
        }
        if not audit_id:
            if source_file is None:
                raise ValidationError("Source file is required.")
            source_split = source_file.name.split(".")
            if len(source_split) < 2:
                raise ValidationError("Invalid source file. Expected CSV file. Received {}.".format(source_file))
            source_type = source_split[1]
            if source_type.lower() != "csv":
                raise ValidationError(
                    "Invalid source file type. Expected CSV file. Received {} file.".format(source_type))
            # Put Source File on S3
            if source_file:
                params["seed_file"] = self.put_source_file_on_s3(source_file)
                params["files"]["source"] = source_file.name

        # Load Keywords from Inclusion File
        if inclusion_file:
            params["inclusion"] = self.load_keywords(inclusion_file)
            params["files"]["inclusion"] = inclusion_file.name
            try:
                params["inclusion_size"] = len(params["inclusion"])
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                pass
        # Load Keywords from Exclusion File
        if exclusion_file:
            try:
                params["exclusion"], params["exclusion_category"] = self.load_exclusion_keywords(exclusion_file)
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                raise ValidationError(
                    "Exclusion file includes invalid / unparsable characters.  Please check and try again.")
            params["files"]["exclusion"] = exclusion_file.name
            try:
                params["exclusion_size"] = len(params["exclusion"])
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
        if category:
            c = []
            for a in json.loads(category):
                c.append(int(a))
            category = c
            params["category"] = category
        if language:
            l = []
            for a in json.loads(language):
                l.append(a)
            language = l
            params["language"] = language
        if related_audits:
            c = []
            for a in json.loads(related_audits):
                c.append(int(a))
            related_audits = c
            params["related_audits"] = related_audits
        if audit_id:
            audit = AuditProcessor.objects.get(id=audit_id)
            if inclusion_file:
                audit.params["inclusion"] = params["inclusion"]
                audit.params["files"]["inclusion"] = params["files"]["inclusion"]
            if exclusion_file:
                audit.params["exclusion"] = params["exclusion"]
                audit.params["files"]["exclusion"] = params["files"]["exclusion"]
                audit.params["exclusion_category"] = params["exclusion_category"]
            if name:
                audit.params["name"] = name
                audit.name = name.lower()
            if max_recommended:
                audit.max_recommended = max_recommended
                audit.completed = None
            audit.params["language"] = language
            audit.params["force_data_refresh"] = force_data_refresh
            audit.params["min_likes"] = min_likes
            audit.params["min_date"] = min_date
            audit.params["min_views"] = min_views
            audit.params["max_dislikes"] = max_dislikes
            audit.params["category"] = category
            audit.params["related_audits"] = related_audits
            audit.params["num_videos"] = num_videos
            audit.params["max_recommended_type"] = max_recommended_type
            audit.params["exclusion_hit_count"] = exclusion_hit_count
            audit.params["inclusion_hit_count"] = inclusion_hit_count
            audit.params["include_unknown_views"] = include_unknown_views
            audit.params["include_unknown_likes"] = include_unknown_likes
            audit.params["override_blocklist"] = override_blocklist
            audit.save()
        else:
            audit = AuditProcessor.objects.create(
                audit_type=audit_type,
                params=params,
                max_recommended=max_recommended
            )
            audit.name = name.lower()
            audit.save(update_fields=["name"])
        return Response(audit.to_dict())

    @staticmethod
    def put_source_file_on_s3(file):
        # take the file uploaded locally, put on S3 and return the s3 filename
        random_file_name = uuid4().hex
        AuditFileS3Exporter.export_to_s3(file, random_file_name)
        return AuditFileS3Exporter.get_s3_key(random_file_name)

    def load_keywords(self, uploaded_file):
        file = uploaded_file.read().decode("utf-8-sig", errors="ignore")
        keywords = []
        io_string = StringIO(file)
        reader = csv.reader(io_string, delimiter=";", quotechar="|")
        for row in reader:
            try:
                word = row[0].lower().strip()
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            if word:
                keywords.append(word)
        return keywords

    def load_exclusion_keywords(self, uploaded_file):
        file = uploaded_file.read().decode("utf-8-sig", errors="ignore")
        exclusion_data = []
        categories = []
        io_string = StringIO(file)
        reader = csv.reader(io_string, delimiter=",", quotechar="\"")
        for row in reader:
            try:
                word = row[0].lower().strip()
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                continue
            try:
                category = row[1].lower().strip()
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                category = ""
            language = self.find_language(row)
            row_data = [word, category, language]
            if word:
                exclusion_data.append(row_data)
                categories.append(category)
        return exclusion_data, categories

    def find_language(self, row):
        try:
            language = row[2].lower().strip()
            if language in LANGUAGES:
                return language
            if not self.LANGUAGES_REVERSE:
                for k, v in LANGUAGES.items():
                    self.LANGUAGES_REVERSE[v.lower()] = k
            if language in self.LANGUAGES_REVERSE:
                return self.LANGUAGES_REVERSE[language]
            return ""
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            return ""

    def patch(self, request):
        """
        Update AuditProcessor fields
        AuditProcessor params will always be updated with new data if provided
        """
        data = request.data
        audit_id = data.get("audit_id")
        segment_id = data.get("segment_id")
        if not audit_id and not segment_id:
            raise ValidationError("You must provide a segment_id or audit_id.")
        try:
            if segment_id:
                segment = CustomSegment.objects.get(id=segment_id)
            else:
                segment = CustomSegment.objects.get(audit_id=audit_id)
        except KeyError:
            raise ValidationError("You must provide a audit_id.")
        except CustomSegment.DoesNotExist:
            raise ValidationError(f"Segment does not exist with parameters: {data}.")

        # If segment does not contain any items, then reject audit creation
        if segment.statistics.get("items_count", 0) <= 0 or getattr(segment, "export", None) is None:
            raise ValidationError(f"The list: {segment.title} does not contain any items. Please create a new list.")
        audit, created = AuditProcessor.objects.get_or_create(id=segment.audit_id, defaults={
            "audit_type": segment.config.AUDIT_TYPE,
            "source": 1,
        })
        if created:
            segment.audit_id = audit.id
            segment.save()
            generate_audit_items.delay(segment.id, data_field=segment.config.DATA_FIELD)
            if not audit.completed:
                audit.completed = timezone.now()
                audit.save(update_fields=["completed"])
        # Update params with instructions
        audit.params.update({
            "instructions": data.pop("instructions", "")
        })
        data["params"] = audit.params
        serializer = AuditProcessorSerializer(audit, data=data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        res = {
            "instructions": serializer.data["params"].get("instructions")
        }
        return Response(data=res)


class AuditFileS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_AUDITS_FILES_BUCKET_NAME
    export_content_type = "application/CSV"

    @classmethod
    def get_s3_key(cls, name):
        key = name
        return key

    @classmethod
    def export_file_to_s3(cls, filename, name):
        cls._s3().upload_file(
            Bucket=cls.bucket_name,
            Key=cls.get_s3_key(name),
            Filename=filename,
        )

    @classmethod
    def export_to_s3(cls, exported_file, name):
        cls._s3().put_object(
            Bucket=cls.bucket_name,
            Key=cls.get_s3_key(name),
            Body=exported_file
        )

    @classmethod
    def get_s3_export_csv(cls, name):
        body = cls.get_s3_export_content(name)
        r = body.read()
        try:
            res = r.decode("utf-8-sig")
        except Exception:
            res = r.decode("utf-32")
        return res.split()
