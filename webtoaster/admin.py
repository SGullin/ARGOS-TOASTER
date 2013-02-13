from django.contrib import admin
from app.models import *

class AppAdmin(admin.ModelAdmin):
      list_display    = ['type', 'teamone', 'teamtwo', 'gametime']

admin.site.register(UserProfile, AppAdmin)