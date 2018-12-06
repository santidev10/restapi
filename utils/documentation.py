from django.conf import settings
from django.conf.urls import url
from django.conf.urls.static import static
from django.contrib.auth import views as auth_views
from django.views.generic import RedirectView
from drf_yasg import openapi
from drf_yasg.views import get_schema_view
from rest_framework import permissions
from rest_framework.authentication import SessionAuthentication

__all__ = [
    "urlpatterns",
]

schema_view = get_schema_view(
    openapi.Info(
        title="ViewIQ REST API",
        description="""
        ReDoc version is under the /docs/redoc/ path
        
        AUTH
        To try API endpoints use following instruction:
        1. Go to POST /auth/
        2. Click "Try it out" button
        3. Put your login and password into form
        4. Click "Execute"
        5. Copy token from the response
        6. Click Authorize button
        7. Put your token into the input in following format: Token <token>
            example: Token d2db1fb4411764b83b3d74d828bf67e9ef7a5bd8
        8. Submit the form.
        
        Now you can Try any method as authorized user.
        Note: You can also take Authentication token from the dev console on the main site.
        """,
        default_version="v1",
    ),
    public=True,
    permission_classes=(permissions.IsAdminUser,),
    authentication_classes=(SessionAuthentication,)
)

urlpatterns = [
    url(r"^$", RedirectView.as_view(pattern_name="login")),
    url(r"^login/$", auth_views.login, dict(template_name="login.html"), name="login"),
    url(r"^logout/$", auth_views.logout, dict(next_page="login"), name="logout"),
    url(r"^swagger(?P<format>\.json|\.yaml)$", schema_view.without_ui(cache_timeout=0), name="schema-json"),
    url(r"^swagger/$", schema_view.with_ui("swagger", cache_timeout=0), name="schema-swagger-ui"),
    url(r"^redoc/$", schema_view.with_ui("redoc", cache_timeout=0), name="schema-redoc"),
]

urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
