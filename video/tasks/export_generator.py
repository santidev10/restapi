from channel.utils import VettedParamsAdapter
from es_components.constants import Sections
from es_components.managers import VideoManager
from utils.es_components_api_utils import BrandSafetyParamAdapter
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import FlagsParamAdapter
from utils.es_components_api_utils import SentimentParamAdapter
from video.api.serializers.video_export import VideoListExportSerializer
from video.constants import EXISTS_FILTER
from video.constants import MATCH_PHRASE_FILTER
from video.constants import MUST_NOT_TERMS_FILTER
from video.constants import RANGE_FILTER
from video.constants import TERMS_FILTER


class VideoListDataGenerator(ExportDataGenerator):
    serializer_class = VideoListExportSerializer
    terms_filter = TERMS_FILTER
    must_not_terms_filter = MUST_NOT_TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER
    params_adapters = (BrandSafetyParamAdapter, VettedParamsAdapter, FlagsParamAdapter, SentimentParamAdapter)
    queryset = ESQuerysetAdapter(VideoManager((
        Sections.MAIN,
        Sections.GENERAL_DATA,
        Sections.STATS,
        Sections.ADS_STATS,
        Sections.BRAND_SAFETY,
        Sections.CHANNEL,
    )))