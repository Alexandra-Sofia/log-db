from django.contrib import admin
from django.urls import path
from .views import parse_and_upload

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", parse_and_upload, name="home"),       # root URL
    path("load/", parse_and_upload, name="parse_and_upload"),
]
