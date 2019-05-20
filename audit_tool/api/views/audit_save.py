from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor
import csv

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

        if str(audit_type) not in AuditProcessor.AUDIT_TYPES:
            raise PROBLEM
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
        pass

    def load_keywords(self, file_name):
        keywords = []
        with open(file_name) as f:
            reader = csv.reader(f)
            for row in reader:
                word = row[0].lower().strip()
                if word:
                    keywords.append(word)
        return keywords