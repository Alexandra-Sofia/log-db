
def isUserLoggedIn(request): 
    return request.user.is_authenticated == True

def getUser(request):
    return {
        "isUserAuthenticated": request.user.is_authenticated,
        "user": request.user.username
    }