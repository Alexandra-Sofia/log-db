import psycopg2

from django.shortcuts import render, redirect
from .util.logger import logger
from django.contrib.auth import login, logout
from ui.forms import CustomLoginForm, CustomUserCreationForm
from .helpers.all import isUserLoggedIn, hasQuery, getQueriesDictionary, executeQueryAndGetResults, getContext

def loginHandler(request):
    if request.method == 'POST':
        form = CustomLoginForm(data=request.POST)
        if form.is_valid():
            user = form.get_user()
            login(request, user)
            return redirect('/')
    else:
        form = CustomLoginForm()
    context = getContext(request) | {'form': form}
    return render(request, 'ui/login.html', context)


def registerHandler(request):
    if request.method == 'POST':
        form = CustomUserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request,user)
            return redirect('/')
    else:
        form = CustomUserCreationForm()
    context = getContext(request) | {'form': form}
    return render(request, 'ui/register.html', context)

def logoutHandler(request):
    logout(request)
    return redirect('/')


def urlHandler(request):
    context = getContext(request)
    if(isUserLoggedIn(request)):
        context = context | getQueriesDictionary()
        return render(request, "ui/queries.html", context)
    else:
        return render(request, "ui/template.html", context)

def queriesHandler(request):
    context = getContext(request)
    if(isUserLoggedIn(request) and hasQuery(request)):
        context = context | executeQueryAndGetResults(request)
        return render(request, "ui/results.html", context)
    return redirect("/")
