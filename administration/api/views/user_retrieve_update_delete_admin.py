from django.contrib.auth import get_user_model
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from administration.api.serializers import UserSerializer


class UserRetrieveUpdateDeleteAdminApiView(RetrieveUpdateDestroyAPIView):
    """
    Admin user delete endpoint
    """
    permission_classes = (IsAdminUser,)
    serializer_class = UserSerializer
    queryset = get_user_model().objects.all()

    def put(self, request, *args, **kwargs):
        """
        Update user
        """
        user = self.get_object()
        access = request.data.pop("access", None)
        if access:
            user.update_access(access)
        return Response(data=self.serializer_class(user).data)
