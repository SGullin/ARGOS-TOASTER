from django.contrib import admin
from app.models import *

class AppAdmin(admin.ModelAdmin):
  list_display    = ['user', 'oauth_token', 'oauth_token_secret']

admin.site.register(UserProfile, AppAdmin)