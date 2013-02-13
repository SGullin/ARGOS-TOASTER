from django.http import Http404
from django.http import Http404
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import *
from django.template import Context, RequestContext
from django.template import loader
from app.models import *
from httplib import HTTPResponse

from django.contrib.auth.models import User
from django.contrib.auth import login
from django.contrib.auth import logout
import oauth2

from django.contrib.auth.decorators import login_required

@login_required
def index(request):
	# user = User.objects.filter(username=u'snip3')[0]
	# user.backend = 'django.contrib.auth.backends.ModelBackend'
	# login( request, user)
	# print "%s" % str(user)


	t = loader.get_template('home/index.html')
	c = RequestContext(request, {
		'welcome_message': 'Welcome to Web-Toaster, friend!',
	})
	return HttpResponse(t.render(c))