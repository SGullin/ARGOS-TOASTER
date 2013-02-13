from django.http import Http404
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import *
from django.template import Context, RequestContext
from django.template import loader
from app.models import *
from httplib import HTTPResponse

def index(request):
	t = loader.get_template('about/index.html')
	c = RequestContext(request, {
		'welcome_message': 'Welcome to Web-Toaster, friend!',
	})
	return HttpResponse(t.render(c))
