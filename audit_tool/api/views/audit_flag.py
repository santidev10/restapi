from django.core.exceptions import ObjectDoesNotExist
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import  HTTP_200_OK
from rest_framework.views import APIView

from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.models import BrandSafetyFlag
from brand_safety.constants import BRAND_SAFETY_SCORE
from brand_safety.models import BadWordCategory
from audit_tool.models import BlacklistItem
from utils.permissions import user_has_permission
from utils.brand_safety import get_brand_safety_data
from utils.es_components_cache import flush_cache


class AuditFlagApiView(APIView):
    permission_classes = (
        user_has_permission("userprofile.flag_audit"),
    )

    def get(self, request):
        valid_types = [BlacklistItem.VIDEO_ITEM, BlacklistItem.CHANNEL_ITEM]
        query_params = request.query_params

        if "item_type" not in query_params:
            raise ValidationError("Parameter 'item_type' required: '0' for VIDEO_ITEM, '1' for CHANNEL_ITEM")
        else:
            try:
                item_type = int(query_params["item_type"])
                if item_type not in valid_types:
                    raise ValidationError("Invalid value for parameter item_type: '{}'. Value must be either: "
                                          "'0' for VIDEO_ITEM or '1' for CHANNEL_ITEM".format(item_type))
            except Exception as e:
                raise ValidationError("Invalid value for parameter item_type: '{}'. Value must be either: "
                                      "'0' for VIDEO_ITEM or '1' for CHANNEL_ITEM".format(query_params["item_type"]))

        if "item_id" not in query_params:
            raise ValidationError("Parameter 'item_id' required.")
        else:
            item_id = query_params["item_id"]

        flag_categories = {}
        if "flag_categories" not in query_params or len(query_params["flag_categories"]) < 1:
            pass
        else:
            for category_id in query_params["flag_categories"].split(","):
                category_id = category_id.strip()
                try:
                    category = BadWordCategory.objects.get(id=category_id)
                    if category is not None:
                        flag_categories[category_id] = 100
                except ObjectDoesNotExist:
                    raise ValidationError("BadWordCategory object with id: '{}' does not exist. "
                                          "Please enter valid flag_categories values.".format(category_id))
                except SyntaxError:
                    raise ValidationError("'{}' is not a valid BadWordCategory ID. Parameter flag_categories "
                                          "must be comma-separated list of Category IDs, not '{}'."
                                          .format(category_id, query_params["flag_categories"]))

        body = {}
        flag = BlacklistItem.get_or_create(item_id, item_type)

        if len(flag_categories) > 0:
            flag.blacklist_category = flag_categories
            flag.save()
            body["action"] = "BlackListItem created/modified."
            blacklist_data = {flag.item_id: flag.blacklist_category}
        else:
            flag.delete()
            body["action"] = "BlackListItem deleted."
            blacklist_data = {}

        # If video, audit immediately and send overall_score in response
        if item_type == 0:
            auditor = BrandSafetyAudit(discovery=False)
            video_audit = auditor.manual_video_audit([item_id], blacklist_data=blacklist_data)[0]
            overall_score = getattr(video_audit, BRAND_SAFETY_SCORE).overall_score
            body["brand_safety_data"] = get_brand_safety_data(overall_score)
        else:
            # Enqueue channel to be audited
            flush_cache()
            BrandSafetyFlag.enqueue(item_id=item_id, item_type=1)
            body["brand_safety_data"] = get_brand_safety_data(None)

        body["BlackListItemDetails"] = {
            "item_type": flag.item_type,
            "item_id": flag.item_id,
            "blacklist_category": flag.blacklist_category
        }

        return Response(data=body, status=HTTP_200_OK)
