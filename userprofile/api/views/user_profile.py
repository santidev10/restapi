from rest_framework.response import Response
from rest_framework.views import APIView

from userprofile.api.serializers import UserSerializer
from userprofile.api.views.user_finalize_response import UserFinalizeResponse


class UserProfileApiView(UserFinalizeResponse, APIView):
    """
    User profile api view
    """
    serializer_class = UserSerializer

    def get(self, request):
        """
        Retrieve profile
        """
        response_data = self.serializer_class(request.user).data
        return Response(response_data)

    def put(self, request):
        """
        Update profile
        """
        data = self.request.data
        if "phone_number" in data:
            data["phone_number_verified"] = True
        serializer = self.serializer_class(
            instance=request.user, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data)
