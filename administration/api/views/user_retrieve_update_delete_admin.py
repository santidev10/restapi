from django.contrib.auth import get_user_model
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from administration.api.serializers import UserSerializer
from administration.api.serializers import UserUpdateSerializer


class UserRetrieveUpdateDeleteAdminApiView(RetrieveUpdateDestroyAPIView):
    """
    Admin user delete endpoint
    """
    permission_classes = (IsAdminUser,)
    serializer_class = UserSerializer
    update_serializer_class = UserUpdateSerializer
    queryset = get_user_model().objects.all()

    def put(self, request, *args, **kwargs):
        """
        Update user
        """
        user = self.get_object()
        serializer = self.update_serializer_class(
            instance=user,
            data=request.data,
            partial=True,
            context={"request": request},
        )
        if not serializer.is_valid():
            return Response(data=serializer.errors, status=HTTP_400_BAD_REQUEST)
        serializer.save()
        return Response(data=self.serializer_class(user).data)
