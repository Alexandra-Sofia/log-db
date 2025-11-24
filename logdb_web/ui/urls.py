from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('parse/', views.parse_logs, name='parse_logs'),
]
