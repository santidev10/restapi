from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView
from ftux.models import FTUX
from ftux.models import FTUXUser

class FTUXRetrieveAPIView(APIView):
    def get(self, request):
        query_params = request.query_params
        feature = query_params.get('feature')
        if not feature:
            raise ValidationError("must provide a 'feature' to get the FTUX for.")
        try:
            valid_ftux = FTUX.objects.get(feature=feature)
        except Exception:
            raise ValidationError("invalid 'feature', please check and try again.")
        ftux_user, new = FTUXUser.objects.get_or_create(
            user_id=request.user.id,
            ftux=valid_ftux,
        )
        res = valid_ftux.to_dict()
        res['show_ftux'] = False
        if not ftux_user.last_seen or ftux_user.last_seen < valid_ftux.refresh_time:
            res['show_ftux'] = True
            ftux_user.last_seen = timezone.now()
            ftux_user.save(update_fields=['last_seen'])
        return Response(res)