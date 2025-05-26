from django.contrib import admin
from cache.models import Reel, Sound


@admin.register(Reel)
class ReelAdmin(admin.ModelAdmin):
    pass


@admin.register(Sound)
class SoundAdmin(admin.ModelAdmin):
    search_fields = ['airtabel_id', 'rapid_id']
