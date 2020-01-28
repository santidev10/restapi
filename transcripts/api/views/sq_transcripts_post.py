import json

from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError


class SQTranscriptsPostApiView(RetrieveUpdateDestroyAPIView):
    def post(self, request):
        # Post Request takes a body, which is a list of dicts, with the key being video_id and value being transcript
        body = json.loads(request.data)
        try:
            for item in body:
                if len(item) != 1:
                    raise ValidationError(f"Only 1 Video Id and Transcript key-value pair expected per object."
                                          f"Got {len(item)}.")
                for video_id in item:
                    transcript = item[video_id]

        except Exception as e:
            raise ValidationError(e)
        pass
