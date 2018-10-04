from django.contrib.auth import get_user_model
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from administration.api.serializers import UserSerializer, UserUpdateSerializer


class UserRetrieveUpdateDeleteAdminApiView(RetrieveUpdateDestroyAPIView):
    """
    Admin user delete endpoint
    """
    permission_classes = (IsAdminUser,)
    serializer_class = UserSerializer
    update_serializer_class = UserUpdateSerializer

    def get_queryset(self):
        """
        All users queryset
        """
        return get_user_model().objects.all()

    def put(self, request, *args, **kwargs):
        """
        Update user
        """
        access = request.data.pop('access', None)
        user = self.get_object()
        serializer = self.update_serializer_class(
            instance=user, data=request.data,
            partial=True, context={"request": request})
        if serializer.is_valid():
            serializer.save()
            if access:
                user.update_access(access)
            return self.get(request)
        return Response(serializer.errors, HTTP_400_BAD_REQUEST)