"""
Administration api views module
"""
import operator
import stripe
from functools import reduce

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db.models import Q, Value
from django.db.models.functions import Concat

from rest_framework.authtoken.models import Token
from rest_framework.generics import ListAPIView, DestroyAPIView, \
    ListCreateAPIView, RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_403_FORBIDDEN, \
    HTTP_201_CREATED, HTTP_400_BAD_REQUEST, HTTP_200_OK
from rest_framework.views import APIView

from administration.api.serializers import UserActionRetrieveSerializer, \
    UserActionCreateSerializer, UserUpdateSerializer
from administration.api.serializers import UserSerializer
from userprofile.api.serializers import UserSerializer as RegularUserSerializer
from administration.models import UserAction
from userprofile.api.serializers import PlanSerializer, SubscriptionSerializer
from userprofile.models import UserProfile, Plan, Subscription
from utils.api_paginator import CustomPageNumberPaginator

from payments.stripe_api import customers, subscriptions
from payments.models import Subscription as PaymentsSubscription


class UserPaginator(CustomPageNumberPaginator):
    """
    Paginator for user list
    """
    page_size = 10


class UserActionPaginator(CustomPageNumberPaginator):
    """
    Paginator for user action list
    """
    page_size = 20


class UserListAdminApiView(ListAPIView):
    """
    Admin user list endpoint
    """
    serializer_class = UserSerializer
    pagination_class = UserPaginator
    permission_classes = (IsAdminUser, )

    def get_queryset(self):
        """
        Exclude requested user from queryset
        """
        queryset = get_user_model().objects.exclude(id=self.request.user.id)
        queryset = self.do_filters(queryset)
        queryset = self.do_sorts(queryset)
        return queryset

    def do_filters(self, queryset):
        search = self.request.query_params.get("search")
        if search:
            search = search.strip()
            queryset = queryset.annotate(full_name=Concat('first_name', Value(' '), 'last_name'))
            queryset = queryset.filter(
                Q(full_name__icontains=search) |
                Q(email__icontains=search) |
                Q(company__icontains=search) |
                Q(phone_number__icontains=search)
            ).distinct()
        return queryset

    def do_sorts(self, queryset):
        return queryset.order_by('last_name', 'first_name')


class UserRetrieveUpdateDeleteAdminApiView(RetrieveUpdateDestroyAPIView):
    """
    Admin user delete endpoint
    """
    permission_classes = (IsAdminUser, )
    serializer_class = UserSerializer
    update_serializer_class = UserUpdateSerializer

    def get_queryset(self):
        """
        Exclude requested user from queryset
        """
        return get_user_model().objects.exclude(id=self.request.user.id)

    def put(self, request, *args, **kwargs):
        """
        Update user
        """
        serializer = self.update_serializer_class(
            instance=self.get_object(), data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return self.get(request)
        return Response(serializer.errors, HTTP_400_BAD_REQUEST)


class AuthAsAUserAdminApiView(APIView):
    """
    Login as a user endpoint
    """
    permission_classes = (IsAdminUser, )

    def get(self, request, pk):
        """
        Get the selected user and return its data
        """
        try:
            user = UserProfile.objects.get(pk=pk)
        except UserProfile.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        Token.objects.get_or_create(user=user)
        response_data = RegularUserSerializer(user).data
        return Response(response_data)


class UserActionListCreateApiView(ListCreateAPIView):
    """
    User action list / create endpoint
    """
    pagination_class = UserActionPaginator
    serializer_class = UserActionRetrieveSerializer
    create_serializer_class = UserActionCreateSerializer
    permission_classes = tuple()

    def post(self, request, *args, **kwargs):
        """
        Add current user to post data
        """
        # add user id to data
        data = request.data.copy()
        data["user"] = request.user.id
        # serialization procedure
        serializer_class = self.create_serializer_class
        kwargs['context'] = self.get_serializer_context()
        serializer = serializer_class(data=data, *args, **kwargs)
        serializer.is_valid(raise_exception=True)
        instance = serializer.save()
        headers = self.get_success_headers(serializer.data)
        return Response(
            self.serializer_class(instance).data,
            status=HTTP_201_CREATED, headers=headers)

    def get(self, request, *args, **kwargs):
        """
        Check admin permission
        """
        # opened for all according UI request
        # if not request.user.is_staff:
        #     return Response(status=HTTP_403_FORBIDDEN)
        return super(UserActionListCreateApiView, self).get(
            request, *args, **kwargs)

    def do_filters(self, queryset):
        """
        Apply filters for queryset
        """
        # search by user
        search = self.request.query_params.get("username")
        if search:
            search = search.strip()
            search = search.split(" ")
            first_name_search = reduce(
                operator.or_, (
                    Q(user__first_name__icontains=x) for x in search))
            last_name_serach = reduce(
                operator.or_, (
                    Q(user__last_name__icontains=x) for x in search))
            email_search = reduce(
                operator.or_, (
                    Q(user__email__icontains=x) for x in search))
            queryset = queryset.filter(
                first_name_search | last_name_serach | email_search)
        filters = {}
        # url
        url = self.request.query_params.get("url")
        if url:
            filters["url__icontains"] = url
        # slug
        slug = self.request.query_params.get("slug")
        if slug:
            filters["slug__icontains"] = slug
        # start date
        start_date = self.request.query_params.get("start_date")
        if start_date:
            filters["created_at__gte"] = start_date
        # end date
        end_date = self.request.query_params.get("end_date")
        if end_date:
            filters["created_at__lte"] = end_date
        if filters:
            try:
                queryset = queryset.filter(**filters)
            except ValidationError:
                queryset = UserAction.objects.none()
        return queryset

    def do_sorts(self, queryset):
        """
        Apply sorts for queryset
        """
        order_by = self.request.query_params.get("order_by")
        if not order_by:
            return queryset
        available_sorts = {
            "slug",
            "url"
        }
        available_reverse_sorts = {
            "created_at"
        }
        if order_by in available_sorts:
            return queryset.order_by("{}".format(order_by))
        elif order_by in available_reverse_sorts:
            return queryset.order_by("-{}".format(order_by))
        return queryset

    def paginate_queryset(self, queryset):
        """
        Processing flat query param
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)

    def get_queryset(self):
        """
        Prepare queryset
        """
        queryset = UserAction.objects.all()
        return self.do_sorts(self.do_filters(queryset))


class UserActionDeleteAdminApiView(DestroyAPIView):
    """
    User action delete endpoint
    """
    permission_classes = (IsAdminUser, )
    queryset = UserAction.objects.all()


class PlanListCreateApiView(ListCreateAPIView):
    permission_classes = tuple()
    serializer_class = PlanSerializer
    create_serializer_class = PlanSerializer
    queryset = Plan.objects.exclude(hidden=True).all()


class PlanChangeDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = (IsAdminUser, )
    serializer_class = PlanSerializer
    queryset = Plan.objects.all()

    def delete(self, request, *args, **kwargs):
        plan = self.get_object()
        if plan.name == settings.DEFAULT_ACCESS_PLAN or plan.hidden:
            return Response(status=HTTP_403_FORBIDDEN)
        default_plan, created = Plan.objects.get_or_create(
            name=settings.DEFAULT_ACCESS_PLAN,
            defaults=settings.ACCESS_PLANS[settings.DEFAULT_ACCESS_PLAN])
        UserProfile.objects.filter(plan=plan).update(plan=default_plan)
        return super().delete(request, *args, **kwargs)


class SubscriptionView(APIView):
    serializer_class = SubscriptionSerializer

    def get(self, request):
        try:
            current_subscription = Subscription.objects.get(user=self.request.user)
        except Subscription.DoesNotExist:
            current_subscription = Subscription.objects.create(
                user=self.request.user,
                plan=self.request.user.plan,
                payments_subscription=self.get_current_subscription()
            )

        serializer = self.serializer_class(current_subscription)
        return Response(serializer.data, status=HTTP_200_OK)

    def get_current_subscription(self):
        customer = customers.get_customer_for_user(self.request.user)
        if customer is None:
            return None

        try:
            subscriptions = customer.subscription_set.all()
        except Subscription.DoesNotExist:
            return None

        if len(subscriptions) > 0:
            return subscriptions[0]

        return None


class SubscriptionCreateView(APIView):
    serializer_class = SubscriptionSerializer

    def subscribe(self, customer, plan, token):
        return subscriptions.create(customer, plan, token=token)

    def post(self, request):
        plan_name = request.data.get('plan')
        if plan_name:
            iq_plan = Plan.objects.get(name=plan_name)
            plan = iq_plan.payments_plan
            payments_subscription = None
            token = request.data.get('token')
            if plan and token:
                try:
                    customer = customers.create(request.user)
                    payments_subscription = self.subscribe(customer, plan=plan, token=token)
                except stripe.StripeError as e:
                    return Response(
                        status=HTTP_400_BAD_REQUEST,
                        data=e.json_body)

            Subscription.objects.filter(user=request.user).delete()
            current_subscription = Subscription.objects.create(
                user=request.user,
                plan=iq_plan,
                payments_subscription=payments_subscription
            )
            request.user.update_permissions_from_subscription(current_subscription)
            serializer = self.serializer_class(current_subscription)
            return Response(serializer.data, status=HTTP_200_OK)


class SubscriptionDeleteView(APIView):
    def post(self, request, *args, **kwargs):
        stripe_id = request.data.get('id')
        # in case that we want to immediately cancel sub we could send at_period_at param
        obj = PaymentsSubscription.objects.get(stripe_id=stripe_id)

        try:
            subscription = Subscription.objects.get(payments_subscription__stripe_id=stripe_id)
        except Subscription.DoesNotExist:
            subscription = None

        try:
            subscriptions.cancel(obj)
        except stripe.StripeError as e:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=e.json_body)

        if subscription:
            user_id = subscription.user_id
            subscription.delete()

            plan = Plan.objects.get(name=settings.DEFAULT_ACCESS_PLAN)
            subscription = Subscription.objects.create(user_id=user_id, plan=plan)
            get_user_model().objects.get(id=user_id).update_permissions_from_subscription(subscription)

        return Response(status=HTTP_200_OK)


class SubscriptionUpdateView(APIView):
    def post(self, request, *args, **kwargs):
        stripe_id = request.data.get('id')
        plan_name = request.data.get('plan')
        if plan_name:
            iq_plan = Plan.objects.get(name=plan_name)
            if iq_plan.payments_plan:
                try:
                    obj = PaymentsSubscription.objects.get(stripe_id=stripe_id)
                    subscriptions.update(obj, iq_plan.payments_plan.id)
                except stripe.StripeError as e:
                    return Response(
                        status=HTTP_400_BAD_REQUEST,
                        data=e.json_body)
            subscription = Subscription.objects.get(user=request.user)
            subscription.plan = iq_plan
            subscription.save()
            request.user.update_permissions_from_subscription(subscription)
        return Response(status=HTTP_200_OK)


