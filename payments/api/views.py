import stripe
from django.utils.encoding import smart_str
from rest_framework.generics import ListAPIView, GenericAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from payments.api.serializers import SubscriptionSerializer, PlanSerializer
from payments.models import Subscription, Plan
from payments.stripe_api import customers, subscriptions


class CustomerMixin:
    @property
    def customer(self):
        if not hasattr(self, "_customer"):
            self._customer = customers.get_customer_for_user(self.request.user)
        return self._customer

    def get_queryset(self):
        return super(CustomerMixin, self).get_queryset().filter(customer=self.customer)


class PlanView(ListAPIView):
    serializer_class = PlanSerializer
    queryset = Plan.objects.all()


class SubscriptionView(APIView):
    serializer_class = SubscriptionSerializer

    def get_current_subscription(self):
        try:
            return self.request.user.customer.subscription_set.all()
        except Subscription.DoesNotExist:
            return None

    def get(self, request):
        current_subscription = self.get_current_subscription()
        if current_subscription:
            serializer = self.serializer_class(current_subscription, many=True)
            return Response(serializer.data, status=HTTP_200_OK)
        return Response(status=HTTP_404_NOT_FOUND)


class SubscriptionCreateView(APIView, CustomerMixin):
    def set_customer(self, request):
        if self.customer is None:
            self._customer = customers.create(request.user)

    def subscribe(self, customer, plan, token):
        subscriptions.create(customer, plan, token=token)

    def post(self, request):
        self.set_customer(request)
        plan = request.data.get('plan')
        token = request.data.get('token')
        if plan and token:
            try:
                self.subscribe(self.customer, plan=plan, token=token)
                return Response(status=HTTP_200_OK)
            except stripe.StripeError as e:
                return Response(data=smart_str(e))


class SubscriptionDeleteView(GenericAPIView, CustomerMixin):
    queryset = Subscription.objects.all()

    def cancel(self):
        subscriptions.cancel(self.object)

    def post(self, request, *args, **kwargs):
        # in case that we want to immediately cancel sub we could send at_period_at param
        self.object = self.get_object()
        try:
            self.cancel()
            return Response(status=HTTP_200_OK)
        except stripe.StripeError as e:
            return Response(data=smart_str(e))


class SubscriptionUpdateView(GenericAPIView, CustomerMixin):
    queryset = Subscription.objects.all()

    def update_subscription(self, plan_id):
        subscriptions.update(self.object, plan_id)

    def post(self, request, *args, **kwargs):
        plan = request.data.get('plan')
        self.object = self.get_object()
        if plan:
            try:
                self.update_subscription(plan_id=plan)
                return Response(status=HTTP_200_OK)
            except stripe.StripeError as e:
                return Response(data=smart_str(e))
