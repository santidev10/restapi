from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from audit_tool.models import AuditProcessor
import csv
from uuid import uuid4
from io import StringIO
from distutils.util import strtobool
import json
from django.conf import settings
from utils.aws.s3_exporter import S3Exporter
from datetime import datetime
from utils.permissions import user_has_permission

class AuditSaveApiView(APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    def post(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        user_id = query_params["user_id"] if "user_id" in query_params else None
        do_videos = strtobool(query_params["do_videos"]) if "do_videos" in query_params else None
        move_to_top = strtobool(query_params["move_to_top"]) if "move_to_top" in query_params else None
        name = query_params["name"] if "name" in query_params else None
        audit_type = int(query_params["audit_type"]) if "audit_type" in query_params else None
        category = query_params["category"] if "category" in query_params else None
        related_audits = query_params["related_audits"] if "related_audits" in query_params else None
        source_file = request.data['source_file'] if "source_file" in request.data else None
        exclusion_file = request.data["exclusion_file"] if "exclusion_file" in request.data else None
        inclusion_file = request.data["inclusion_file"] if "inclusion_file" in request.data else None
        min_likes = query_params["min_likes"] if "min_likes" in query_params else None
        min_views = query_params["min_views"] if "min_views" in query_params else None
        max_dislikes = query_params["max_dislikes"] if "max_dislikes" in query_params else None
        min_date = query_params["min_date"] if "min_date" in query_params else None
        if min_date:
            if '/' not in min_date:
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
                audit = AuditProcessor.objects.get(id=audit_id)
                lowest_priority = AuditProcessor.objects.filter(completed__isnull=True).exclude(id=audit_id).order_by("pause")[0]
                audit.pause = lowest_priority.pause - 1
                audit.save(update_fields=['pause'])
                return Response(audit.to_dict())
            except Exception as e:
                raise ValidationError("invalid audit_id")
        try:
            max_recommended = int(query_params["max_recommended"]) if "max_recommended" in query_params else 100000
            if max_recommended > 500000:
                max_recommended = 500000
        except ValueError:
            raise ValidationError("Expected max_recommended ({}) to be <int> type object. Received object of type {}."
                                  .format(query_params["max_recommended"], type(query_params["max_recommended"])))
        language = query_params["language"] if "language" in query_params else 'en'

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
            'name': name,
            'language': language,
            'user_id': user_id,
            'do_videos': do_videos,
            'category': category,
            'related_audits': related_audits,
            'audit_type_original': audit_type,
            'min_likes': min_likes,
            'min_date': min_date,
            'min_views': min_views,
            'max_dislikes': max_dislikes,
        }
        if not audit_id:
            if source_file is None:
                raise ValidationError("Source file is required.")
            if source_file:
                source_split = source_file.name.split(".")
            if len(source_split) < 2:
                raise ValidationError("Invalid source file. Expected CSV file. Received {}.".format(source_file))
            source_type = source_split[1]
            if source_type.lower() != "csv":
                raise ValidationError("Invalid source file type. Expected CSV file. Received {} file.".format(source_type))
            # Put Source File on S3
            if source_file:
                params['seed_file'] = self.put_source_file_on_s3(source_file)

        # Load Keywords from Inclusion File
        if inclusion_file:
            params['inclusion'] = self.load_keywords(inclusion_file)
        # Load Keywords from Exclusion File
        if exclusion_file:
            params['exclusion'] = self.load_keywords(exclusion_file)
        if category:
            c = []
            for a in json.loads(category):
                c.append(int(a))
            category = c
            params['category'] = category
        if related_audits:
            c = []
            for a in json.loads(related_audits):
                c.append(int(a))
            related_audits = c
            params['related_audits'] = related_audits
        if audit_id:
            audit = AuditProcessor.objects.get(id=audit_id)
            if inclusion_file:
                audit.params['inclusion'] = params['inclusion']
            if exclusion_file:
                audit.params['exclusion'] = params['exclusion']
            if name:
                audit.params['name'] = name
                audit.name = name.lower()
            if max_recommended:
                audit.max_recommended = max_recommended
                audit.completed = None
            if language:
                audit.params['language'] = language
            audit.params['min_likes'] = min_likes
            audit.params['min_date'] = min_date
            audit.params['min_views'] = min_views
            audit.params['max_dislikes'] = max_dislikes
            audit.params['category'] = category
            audit.params['related_audits'] = related_audits
            audit.save()
        else:
            audit = AuditProcessor.objects.create(
                audit_type=audit_type,
                params=params,
                max_recommended=max_recommended
            )
            audit.name = name.lower()
            audit.save(update_fields=['name'])
        return Response(audit.to_dict())

    @staticmethod
    def put_source_file_on_s3(file):
        # take the file uploaded locally, put on S3 and return the s3 filename
        random_file_name = uuid4().hex
        AuditFileS3Exporter.export_to_s3(file, random_file_name)
        return AuditFileS3Exporter.get_s3_key(random_file_name)

    def load_keywords(self, uploaded_file):
        file = uploaded_file.read().decode('utf-8-sig')
        keywords = []
        io_string = StringIO(file)
        reader = csv.reader(io_string, delimiter=";", quotechar="|")
        for row in reader:
            try:
                word = row[0].lower().strip()
            except Exception as e:
                pass
            if word:
                keywords.append(word)
        return keywords


class AuditFileS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_AUDITS_FILES_BUCKET_NAME
    export_content_type = "application/CSV"

    @classmethod
    def get_s3_key(cls, name):
        key = name
        return key

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
        return body.read().decode('utf-8-sig').split()
