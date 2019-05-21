from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor
import csv
from uuid import uuid4
from io import StringIO

from django.conf import settings
from utils.aws.s3_exporter import S3Exporter

S3_AUDIT_EXPORT_KEY_PATTERN = "audits/{file_name}"


class AuditSaveApiView(APIView):
    def post(self, request):
        query_params = request.query_params
        name = query_params["name"] if "name" in query_params else None
        audit_type = int(query_params["audit_type"]) if "audit_type" in query_params else None
        source_file = request.data['source_file'] if "source_file" in request.data else None
        exclusion_file = request.data["exclusion_file"] if "exclusion_file" in request.data else None
        inclusion_file = request.data["inclusion_file"] if "inclusion_file" in request.data else None
        max_recommended = int(query_params["max_recommended"]) if "max_recommended" in query_params else 100000
        language = query_params["language"] if "language" in query_params else 'en'
        # handle source_file upload IF source_file (put in s3)
        # handle exclusion_file upload IF exclusion_file (put in s3)
        # handle inclusion_file upload IF inclusion_file (put in s3)

        # Audit Name Validation
        if len(name) < 3:
            raise ValueError("Name {} must be at least 3 characters long.".format(name))
        # Audit Type Validation
        if str(audit_type) not in AuditProcessor.AUDIT_TYPES:
            raise ValueError("Expected Audit Type to have one of the following values: {}. Received {}.".format(
                AuditProcessor.AUDIT_TYPES, audit_type
            ))
        # Source File Validation
        if source_file:
            source_split = source_file.name.split(".")
        else:
            raise ValueError("Source file required.")
        if len(source_split) < 2:
            raise ValueError("Invalid source file. Expected CSV file. Received {}.".format(source_file))
        source_type = source_split[1]
        if source_type != "csv":
            raise ValueError("Invalid source file type. Expected CSV file. Received {} file.".format(source_type))

        params = {
            'name': name,
            'language': language
        }
        # Put Source File on S3
        params['seed_file'] = self.put_source_file_on_s3(source_file)
        # Load Keywords from Inclusion File
        if inclusion_file:
            params['inclusion'] = self.load_keywords(inclusion_file)
        # Load Keywords from Exclusion File
        if exclusion_file:
            params['exclusion'] = self.load_keywords(exclusion_file)
        audit = AuditProcessor.objects.create(
            audit_type=audit_type,
            params=params,
            max_recommended=max_recommended
        )
        return Response(audit.to_dict())

    def put_source_file_on_s3(self, file):
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
            word = row[0].lower().strip()
            if word:
                keywords.append(word)
        return keywords

class AuditFileS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_AUDITS_BUCKET_NAME
    export_content_type = "application/CSV"

    @staticmethod
    def get_s3_key(name):
        key = S3_AUDIT_EXPORT_KEY_PATTERN.format(file_name=name)
        return key

    @classmethod
    def export_to_s3(cls, exported_file, name):
        S3Exporter._s3().put_object(
            Bucket=cls.bucket_name,
            Key=cls.get_s3_key(name),
            Body=exported_file
        )
