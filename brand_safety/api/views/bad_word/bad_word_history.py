from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.serializers import ValidationError

from datetime import datetime
from datetime import timedelta

from brand_safety.api.serializers.bad_word_history_serializer import BadWordHistorySerializer
from brand_safety.models import BadWordHistory


class BadWordHistoryApiView(ListAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordHistorySerializer

    NUM_ENTRIES = 500

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
        history = self.get_queryset().values('tag__name', 'tag__id', 'action', 'created_at', 'changes')
        for entry in history:
            entry['name'] = entry.pop('tag__name')
            entry['id'] = entry.pop('tag__id')
            entry['action'] = BadWordHistory.ACTIONS[entry['action']]
        return Response(data=history[:self.NUM_ENTRIES], status=HTTP_200_OK)
