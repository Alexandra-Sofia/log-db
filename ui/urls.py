from django.contrib import admin
from django.urls import path
from .views import *

urlpatterns = [
    path("", urlHandler, name="home"),
    path("login", loginHandler, name="login"),
    path("register", registerHandler, name="register"),
    path("logout", logoutHandler, name="logout"),
    path("askQuery", queriesHandler, name="results")
]
