from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.serializers import ValidationError

from datetime import datetime
from datetime import timedelta

from brand_safety.api.serializers.bad_word_history_serializer import BadWordHistorySerializer
from brand_safety.models import BadWordHistory


class BadWordHistoryApiView(ListAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordHistorySerializer

    def do_filters(self, queryset):
        days = self.request.query_params.get('days')
        if days:
            try:
                days = int(days)
            except ValueError:
                raise ValidationError("Expected 'days' to be integer. Received {}.".format(days))
        else:
            days = 7
        return queryset.filter(created_at__gte=datetime.today()-timedelta(days=days))

    def get_queryset(self):
        queryset = BadWordHistory.objects.select_related("tag").all().order_by("-created_at")
        queryset = self.do_filters(queryset)
        return queryset

    def get(self, request, *args, **kwargs):
        history = []
        for object in self.get_queryset():
            try:
                tag_object = object.tag
                entry = {}
                entry['tag'] = tag_object.name
                entry['tag_id'] = tag_object.id
                entry['action'] = object.action
                entry['date'] = object.created_at
                entry['language'] = tag_object.language.language
                entry['category'] = tag_object.category.name
                entry['before'] = object.before
                entry['after'] = object.after
                entry['fields_modified'] = object.fields_modified
                history.append(entry)
            except Exception as e:
                pass
        return Response(data=history, status=HTTP_200_OK)
