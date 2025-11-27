from django.db import models
from django.contrib.auth.models import User

class UserQueryLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    query_text = models.TextField()
    executed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "user_query_log"

