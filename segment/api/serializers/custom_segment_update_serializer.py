from PIL import Image
from django.conf import settings
from rest_framework import serializers
from segment.models.constants import CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
from segment.models.constants import CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY
from segment.models.custom_segment import CustomSegment
from segment.utils.custom_segment_featured_image_s3_exporter import CustomSegmentFeaturedImageS3Exporter
from utils.file_storage.s3_connector import upload_file
import io

class CustomSegmentUpdateSerializer(serializers.Serializer):
    is_featured = serializers.BooleanField()
    is_regenerating = serializers.BooleanField()
    image = serializers.ImageField(write_only=True)
    featured_image_url = serializers.SerializerMethodField(read_only=True)

    IMAGE_FIELD_NAME = 'image'
    FEATURED_IMAGE_URL_FIELD_NAME = 'featured_image_url'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_exporter = CustomSegmentFeaturedImageS3Exporter()

    def get_featured_image_url(self, object):
        """
        serializer method field
        """
        return object.featured_image_url or CUSTOM_SEGMENT_DEFAULT_IMAGE_URL

    def upload_featured_image(self, instance: CustomSegment, pil_image):
        """
        uploads a received form image for a given CustomSegment instance
        uses the instance's UUID to form the filename, and the PIL image's
        extension to build the s3 key. Because the key is the same per
        CustomSegment instance, we don't have to worry about removing a new
        image if a set image gets replaced.
        """
        extension = pil_image.name.split('.')[-1]
        s3_key = CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY.format(
            filename=instance.uuid,
            extension=extension
        )
        image = Image.open(pil_image)
        file = io.BytesIO()
        image.save(file, format=pil_image.image.format)
        bytes = file.getvalue()
        featured_image_url = upload_file(
            filename=s3_key,
            data=bytes,
            content_type=pil_image.content_type,
            bucket=settings.AMAZON_S3_BUCKET_NAME
        )
        return featured_image_url

    def update(self, instance, validated_data):
        """
        update the CustomSegment instance with validated data, upload the image
        and set the featured_image_url if an image is passed
        """
        # upload image field and save featured_image_url
        image = validated_data.get(self.IMAGE_FIELD_NAME, None)
        if image:
            validated_data[self.FEATURED_IMAGE_URL_FIELD_NAME] = self.upload_featured_image(instance, image)
            validated_data.pop(self.IMAGE_FIELD_NAME, None)

        for field, value in validated_data.items():
            setattr(instance, field, value)
        instance.save()
        return instance
