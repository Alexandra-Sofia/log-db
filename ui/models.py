from django.db import models
from django.db.models import Value
from django.contrib.auth.models import User

class UserQueryLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    query_text = models.TextField()
    executed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "user_query_log"

class LogType(models.Model):
    id = models.SmallIntegerField(primary_key=True,default=Value("nextval('log_type_id_seq'::regclass)"))
    name = models.TextField()
    class Meta:
        db_table = "log_type"