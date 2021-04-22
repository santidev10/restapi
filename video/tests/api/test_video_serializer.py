from django.test import TestCase

from es_components.models.video import Video
from utils.unittests.int_iterator import int_iterator
from video.api.serializers.video import VideoSerializer


class VideoSerializerTestCase(TestCase):

    def test_transcript_serialization(self):
        """
        ensure that videos with an empty custom_captions.items fails gracefully, by producing an empty `transcript`
        string on serialization
        :return:
        """
        v1 = Video(**{
            "meta": {
                "id": next(int_iterator),
            },
            "general_data": {
                "title": "title 1",
                "description": "this has an empty custom_captions.items node. Serialization should fail gracefully"
            },
            "custom_captions": {
                "items": []
            }
        })
        v2_transcript = "video two transcript"
        v2 = Video(**{
            "meta": {
                "id": next(int_iterator),
            },
            "general_data": {
                "title": "title 2",
                "description": "this has a valid lang_code, which matches caption language_code. transcript is valid",
                "lang_code": "en",
            },
            "custom_captions": {
                "items": [
                    {
                        "text": v2_transcript,
                        "language_code": "en"
                    }
                ]
            }
        })
        for video, transcript in [(v1, ""), (v2, v2_transcript)]:
            with self.subTest(video):
                serializer = VideoSerializer(video)
                data = serializer.data
                self.assertEqual(data.get("transcript"), transcript)
