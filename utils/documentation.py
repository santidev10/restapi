from django.conf import settings
from django.conf.urls import url, include
from django.conf.urls.static import static
from drf_yasg import openapi
from drf_yasg.views import get_schema_view
from rest_framework import permissions

from brand_safety.api import urls as brand_safety_api_urls
from saas.urls.namespaces import Namespace

__all__ = [
    "urlpatterns",
]

schema_view = get_schema_view(
    openapi.Info(
        title="ViewIQ REST API",
        default_version="v1",
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),
    patterns=[url(r'^api/v1/', include(brand_safety_api_urls, namespace=Namespace.BRAND_SAFETY))],
)

urlpatterns = [
    url(r'^swagger(?P<format>\.json|\.yaml)$', schema_view.without_ui(cache_timeout=0), name='schema-json'),
    url(r'^swagger/$', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    url(r'^redoc/$', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
]

urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
