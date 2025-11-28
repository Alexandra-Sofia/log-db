from .auth import getUser

def getContext(request):
    return getUser(request)
    