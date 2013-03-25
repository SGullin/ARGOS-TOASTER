from django.contrib import admin
from app.models import *

class AppAdmin(admin.ModelAdmin):
  list_display    = ['user', 'toaster_user', 'oauth_token', 'oauth_token_secret']

admin.site.register(UserProfile, AppAdmin)

class ToasterUserAdmin(admin.ModelAdmin):
  list_display = ['user_id', 'userprofile__user', 'user_name', 'real_name', 'email_address', 'passwd_hash','active','admin']
  def userprofile__user(self, instance):
    return instance.userprofile.user
admin.site.register(ToasterUser, ToasterUserAdmin)