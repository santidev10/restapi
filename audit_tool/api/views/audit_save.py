from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor
import csv

from django.conf import settings
from utils.aws.s3_exporter import S3Exporter

S3_AUDIT_EXPORT_KEY_PATTERN = "audits/{file_name}.csv"

class AuditSaveApiView(APIView):
    def post(self, request):
        query_params = request.query_params
        name = query_params["name"] if "name" in query_params else None
        audit_type = int(query_params["audit_type"]) if "audit_type" in query_params else None
        source_file = query_params["source_file"] if "source_file" in query_params else None
        exclusion_file = query_params["exclusion_file"] if "exclusion_file" in query_params else None
        inclusion_file = query_params["inclusion_file"] if "inclusion_file" in query_params else None
        max_recommended = int(query_params["max_recommended"]) if "max_recommended" in query_params else 100000
        language = query_params["language"] if "language" in query_params else 'en'
        # handle source_file upload IF source_file (put in s3)
        # handle exclusion_file upload IF exclusion_file (put in s3)
        # handle inclusion_file upload IF inclusion_file (put in s3)

        if len(name) < 3:
            raise ValueError("Name {} must be at least 3 characters long.".format(name))

        if str(audit_type) not in AuditProcessor.AUDIT_TYPES:
            raise ValueError("Expected Audit Type to have one of the following values: {}. Received {}.".format(
                AuditProcessor.AUDIT_TYPES, audit_type
            ))

        source_split = source_file.split(".")
        if len(source_split) < 2:
            raise ValueError("Invalid source file. Expected CSV file. Received {}.".format(source_file))
        source_type = source_split[1]
        if source_type != "csv":
            raise ValueError("Invalid source file type. Expected CSV file. Received {} file.".format(source_type))

        params = {
            'name': name,
            'audit_type': audit_type,
            'language': language
        }
        params['seed_file'] = self.put_source_file_on_s3(source_file)
        if inclusion_file:
            params['inclusion'] = self.load_keywords(inclusion_file)
        if exclusion_file:
            params['exclusion'] = self.load_keywords(exclusion_file)
        audit = AuditProcessor.objects.create(
            audit_type=audit_type,
            params=params,
            max_recommended=max_recommended
        )
        return Response(audit.to_dict())

    def put_source_file_on_s3(self, file_name):
        # take the file uploaded locally, put on S3 and return the s3 filename
        with open(file_name) as f:
            AuditFileS3Exporter.export_to_s3(f, file_name)
        return AuditFileS3Exporter.get_s3_key(file_name)

    def load_keywords(self, file_name):
        keywords = []
        with open(file_name) as f:
            reader = csv.reader(f)
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
