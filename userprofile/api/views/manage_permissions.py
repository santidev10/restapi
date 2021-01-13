from rest_framework.response import Response
from rest_framework.views import APIView
from userprofile.models import PermissionItem
from userprofile.models import UserProfile

class UserPermissionsManagement(APIView):
    """
    Get User Options & Permissions
    """
    def get(self, request):
        if not UserProfile.has_permission('user_management'):
            return Response({'error': 'You can not do this'})
        user_id = request.params.get('user_id')
        if not user_id:
            raise Exception("Must provide a user_id to manage permissions for.")
        try:
            user = UserProfile.objects.get(id=user_id)
        except Exception as e:
            raise Exception("invalid user id")
        response_data = []
        all_perms = PermissionItem.objects.all()
        for p in all_perms:
            enabled = user.perms.get(p.perm)
            if enabled is None:
                enabled = p.default_value
            response_data.append({
                'perm': p.perm,
                'enabled': enabled,
                'text': p.display
            })
        return Response(response_data)

    # def post(self, request):
    #     """
    #     Update profile
    #     """
    #     data = self.request.data
    #     if "permissions" in data:
    #         data["phone_number_verified"] = True
    #
    #     return Response({'status': 'success'})
