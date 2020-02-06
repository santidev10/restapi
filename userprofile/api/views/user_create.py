from django.contrib.auth import get_user_model
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.views import APIView

from userprofile.api.serializers import UserCreateSerializer
from userprofile.api.serializers import UserSerializer
from userprofile.api.views.user_finalize_response import UserFinalizeResponse


class UserCreateApiView(UserFinalizeResponse, APIView):
    """
    User list / create endpoint
    """
    permission_classes = tuple()
    serializer_class = UserCreateSerializer
    retrieve_serializer_class = UserSerializer
    queryset = get_user_model()

    def post(self, request):
        """
        Extend post functionality
        """
        serializer = self.serializer_class(
            data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        response_data = self.retrieve_serializer_class(user).data
        return Response(response_data, status=HTTP_201_CREATED)
