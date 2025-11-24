from django.db import models


class LogType(models.Model):
    name = models.CharField(max_length=100, unique=True)

    class Meta:
        db_table = "log_type"

    def __str__(self):
        return self.name


class ActionType(models.Model):
    name = models.CharField(max_length=100, unique=True)

    class Meta:
        db_table = "action_type"

    def __str__(self):
        return self.name


class LogEntry(models.Model):
    log_type = models.ForeignKey(LogType, on_delete=models.PROTECT)
    action_type = models.ForeignKey(ActionType, on_delete=models.PROTECT, null=True)

    log_timestamp = models.DateTimeField()

    source_ip = models.GenericIPAddressField(null=True)
    dest_ip = models.GenericIPAddressField(null=True)

    block_id = models.BigIntegerField(null=True)
    size_bytes = models.BigIntegerField(null=True)

    file_name = models.TextField(null=True)
    line_number = models.IntegerField(null=True)

    raw_message = models.TextField(null=True)

    class Meta:
        db_table = "log_entry"

    def __str__(self):
        return f"{self.log_type} @ {self.log_timestamp}"


class LogAccessDetail(models.Model):
    log_entry = models.OneToOneField(LogEntry, on_delete=models.CASCADE, primary_key=True)

    remote_name = models.TextField(null=True)
    auth_user = models.TextField(null=True)
    http_method = models.CharField(max_length=10, null=True)
    resource = models.TextField(null=True)
    http_status = models.IntegerField(null=True)
    referrer = models.TextField(null=True)
    user_agent = models.TextField(null=True)

    class Meta:
        db_table = "log_access_detail"

    def __str__(self):
        return f"Access detail for {self.log_entry_id}"
