from django.db import models


class Reel(models.Model):
    airtabel_id = models.CharField(max_length=32, unique=True, db_index=True)
    rapid_id = models.CharField(max_length=32, unique=True, db_index=True)

    account_username = models.CharField(max_length=64, blank=True)
    model = models.CharField(max_length=32, blank=True)
    video_duration = models.FloatField(default=0, blank=True)
    video_hash = models.CharField(max_length=32, blank=True, db_index=True)
    sound_rapid_id = models.CharField(max_length=32, blank=True)
    sound_type = models.CharField(max_length=32, blank=True)
    code = models.CharField(max_length=32, blank=True)


class Sound(models.Model):
    airtabel_id = models.CharField(max_length=32, unique=True, db_index=True)
    rapid_id = models.CharField(max_length=32, unique=True, db_index=True)


class TwitterUser(models.Model):
    rapid_id = models.CharField(max_length=48, unique=True, db_index=True)
    base = models.CharField(max_length=80, blank=True)
    table = models.CharField(max_length=52, blank=True)
