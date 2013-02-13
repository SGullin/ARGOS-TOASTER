from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save

class UserProfile(models.Model):
	user = models.OneToOneField(User)
	oauth_token = models.CharField(max_length=128)
	oauth_token_secret = models.CharField(max_length=128)
	def __str__(self):
		return "%s's profile. oauth_token: %s oauth_token_secret: %s " % ( self.user , self.oauth_token, self.oauth_token_secret )

def create_user_profile(sender, instance, created, **kwargs):
	if created:
		profile, created = UserProfile.objects.get_or_create(user=instance)

post_save.connect(create_user_profile, sender=User)