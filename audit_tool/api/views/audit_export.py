from distutils.util import strtobool
import csv
import requests

from audit_tool.models import AuditCategory
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditProcessor

from rest_framework.views import APIView
from rest_framework.response import Response

from django.conf import settings
from audit_tool.api.views.audit_save import AuditFileS3Exporter

class AuditExportApiView(APIView):
    CATEGORY_API_URL = "https://www.googleapis.com/youtube/v3/videoCategories" \
                       "?key={key}&part=id,snippet&id={id}"
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY

    def get(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        clean = query_params["clean"] if "clean" in query_params else None

        # Validate audit_id
        if audit_id is None:
            raise ValueError("audit_id is required.")
        if clean is not None:
            clean = strtobool(clean)

        try:
            audit = AuditProcessor.objects.get(id=audit_id)
        except Exception as e:
            raise KeyError("Audit with id {} does not exist.".format(audit_id))

        audit_type = audit.audit_type

        if audit_type == 0 or audit_type == 1:
            url = self.export_videos(audit_id=audit_id, clean=clean)
        elif audit_type == 2:
            pass

        return Response(data=url)

    def get_categories(self):
        categories = AuditCategory.objects.filter(category_display__isnull=True).values_list('category', flat=True)
        url = self.CATEGORY_API_URL.format(key=self.DATA_API_KEY, id=','.join(categories))
        r = requests.get(url)
        data = r.json()
        for i in data['items']:
            AuditCategory.objects.filter(category=i['id']).update(category_display=i['snippet']['title'])

    def export_videos(self, audit_id=None, num_out=None, clean=None):
        audit = AuditProcessor.objects.get(id=audit_id)
        self.get_categories()
        cols = [
            "video ID",
            "name",
            "language",
            "category",
            "views",
            "likes",
            "dislikes",
            "emoji",
            "publish date",
            "channel name",
            "channel ID",
            "channel default lang.",
            "channel subscribers",
            "country",
            "all hit words",
            "unique hit words",
        ]
        video_ids = []
        hit_words = {}
        videos = AuditVideoProcessor.objects.filter(audit_id=audit_id, clean=clean).select_related("video")#.values_list('video_id', flat=True)
        for vid in videos:
            video_ids.append(vid.video_id)
            hit_words[vid.video.video_id] = vid.word_hits
        video_meta = AuditVideoMeta.objects.filter(video_id__in=video_ids).select_related(
                "video",
                "video__channel",
                "video__channel__auditchannelmeta",
                "video__channel__auditchannelmeta__country",
                "language",
                "category"
        )
        if num_out:
            video_meta = video_meta[:num_out]
        try:
            name = self.audit.params['name'].replace("/", "-")
        except Exception as e:
            name = audit_id
        with open('export_{}.csv'.format(name), 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(cols)
            for v in video_meta:
                try:
                    language = v.language.language
                except Exception as e:
                    language = ""
                try:
                    category = v.category.category_display
                except Exception as e:
                    category = ""
                try:
                    country = v.video.channel.auditchannelmeta.country.country
                except Exception as e:
                    country = ""
                try:
                    channel_lang = v.video.channel.auditchannelmeta.language.language
                except Exception as e:
                    channel_lang = ''
                all_hit_words, unique_hit_words = self.get_hit_words(hit_words, v.video.video_id)
                data = [
                    v.video.video_id,
                    v.name,
                    language,
                    category,
                    v.views,
                    v.likes,
                    v.dislikes,
                    'T' if v.emoji else 'F',
                    v.publish_date.strftime("%m/%d/%Y, %H:%M:%S") if v.publish_date else '',
                    v.video.channel.auditchannelmeta.name if v.video.channel else '',
                    v.video.channel.channel_id if v.video.channel else '',
                    channel_lang,
                    v.video.channel.auditchannelmeta.subscribers if v.video.channel else '',
                    country,
                    all_hit_words,
                    unique_hit_words,
                ]
                wr.writerow(data)
            if audit and audit.completed:
                audit.params['export'] = 'export_clean_{}.csv'.format(clean)
                audit.save()
            return 'export_clean_{}.csv'.format(clean)

