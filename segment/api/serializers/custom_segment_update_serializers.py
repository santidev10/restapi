import io

from PIL import Image
from django.conf import settings
from rest_framework import serializers

from audit_tool.models import get_hash_name
from segment.api.serializers.custom_segment_serializer import FeaturedImageUrlMixin
from segment.models.custom_segment import CustomSegment
from utils.file_storage.s3_connector import delete_file
from utils.file_storage.s3_connector import upload_file


__all__ = [
    "CustomSegmentUpdateSerializer",
    "CustomSegmentAdminUpdateSerializer",
]


class CustomSegmentUpdateSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=255)

    def validate(self, data):
        title = data.get("title", None)
        if title:
            data["title_hash"] = get_hash_name(title.lower().strip())
        return data

    def update(self, instance, validated_data):
        """
        update the CustomSegment instance with validated data
        """
        for field, value in validated_data.items():
            setattr(instance, field, value)
        instance.save()
        return instance

    def create(self, validated_data):
        pass


class CustomSegmentAdminUpdateSerializer(FeaturedImageUrlMixin, CustomSegmentUpdateSerializer):
    title = serializers.CharField(max_length=255)
    is_featured = serializers.BooleanField()
    is_regenerating = serializers.BooleanField()
    featured_image = serializers.ImageField(write_only=True)
    featured_image_url = serializers.SerializerMethodField(read_only=True)

    FEATURED_IMAGE_FIELD_NAME = "featured_image"
    FEATURED_IMAGE_URL_FIELD_NAME = "featured_image_url"
    S3_BUCKET = settings.AMAZON_S3_BUCKET_NAME

    def upload_featured_image(self, instance: CustomSegment, pil_image):
        """
        uploads a received form image for a given CustomSegment instance
        uses the instance's UUID to form the filename, and the PIL image's
        extension to build the s3 key. Because the key is the same per
        CustomSegment instance, we don't have to worry about removing a new
        image if a set image gets replaced.
        """
        extension = pil_image.name.split(".")[-1]
        s3_key = CustomSegment.get_featured_image_s3_key(uuid=instance.uuid, extension=extension)
        image = Image.open(pil_image)
        file = io.BytesIO()
        image.save(file, format=pil_image.image.format)
        data_bytes = file.getvalue()
        featured_image_url = upload_file(
            filename=s3_key,
            data=data_bytes,
            content_type=pil_image.content_type,
            bucket=self.S3_BUCKET
        )
        return featured_image_url

    def delete_featured_image(self, instance: CustomSegment, extension):
        """
        deletes the featured image from S3
        """
        s3_key = CustomSegment.get_featured_image_s3_key(uuid=instance.uuid, extension=extension)
        delete_file(filename=s3_key, bucket=self.S3_BUCKET)

    def update(self, instance, validated_data):
        """
        update the CustomSegment instance with validated data, upload the image
        and set the featured_image_url if an image is passed
        """
        # upload image field and save featured_image_url
        featured_image = validated_data.get(self.FEATURED_IMAGE_FIELD_NAME, None)
        if featured_image:
            existing_featured_image_url = getattr(instance, self.FEATURED_IMAGE_URL_FIELD_NAME, None)
            if existing_featured_image_url:
                extension = existing_featured_image_url.split(".")[-1]
                self.delete_featured_image(instance, extension)
            validated_data[self.FEATURED_IMAGE_URL_FIELD_NAME] \
                = self.upload_featured_image(instance, featured_image)
            validated_data.pop(self.FEATURED_IMAGE_FIELD_NAME, None)

        for field, value in validated_data.items():
            setattr(instance, field, value)
        instance.save()
        return instance

    def create(self, validated_data):
        pass
