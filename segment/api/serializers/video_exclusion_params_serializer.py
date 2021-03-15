from rest_framework import serializers
from rest_framework.exceptions import ValidationError

from segment.models.constants import SegmentTypeEnum
from utils.brand_safety import map_score_threshold


class VideoExclusionParamsSerializer(serializers.Serializer):
    with_video_exclusion = serializers.BooleanField(write_only=True)
    video_exclusion_score_threshold = serializers.IntegerField(write_only=True)

    def validate_with_video_exclusion(self, with_video_exclusion):
        """ with_video_exclusion is only allowed with channel CTL """
        source_ctl = self.context["source_ctl"]
        if source_ctl.segment_type == SegmentTypeEnum.VIDEO.value and with_video_exclusion is True:
            raise ValidationError("Video exclusion can only be created with a Channel CTL.")
        return with_video_exclusion

    def validate_score_threshold(self, score_threshold):
        """ If score threshold provided, try and map. It not, then source channel ctl score threshold will be used """
        mapped = map_score_threshold(score_threshold)
        if mapped is None:
            raise ValidationError(f"Invalid score threshold: {score_threshold}")
        return score_threshold
