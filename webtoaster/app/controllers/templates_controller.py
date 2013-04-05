from django.http import Http404
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import *
from django.template import Context, RequestContext
from django.template import loader
from app.models import *
from httplib import HTTPResponse
from lib.toaster import Pulsars
from lib.toaster import Templates

from django.core.context_processors import csrf
from django.conf import settings

from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.core.exceptions import ObjectDoesNotExist
import oauth2

from django.contrib.auth.decorators import login_required
from django import forms

from django.core.paginator import Paginator



class ParfileForm(forms.Form):
  name = forms.CharField()

def index(request):
  templates = Templates.show()

  t = loader.get_template('templates/index.html')
  c = RequestContext(request, {
    'templates': templates,
    })
  return HttpResponse(t.render(c))

def show(request, template_id):
  template_id = int( template_id )
  template = Templates.show( template_id=template_id)[0]
  t = loader.get_template('templates/show.html')
  c = RequestContext(request, {
    'template': template,
    })
  return HttpResponse(t.render(c))

def new(request):
  import os
  if request.method == 'POST' and request.FILES.get('templatefile'):
    try:
      uf = request.FILES['templatefile']
      temp_path = settings.TEMP_DIR
      fn = uf.name
      file_path = os.path.join( temp_path, fn )
      open( file_path, 'w' ).write( uf.read() )
      load_status = Templates.upload( username=request.user.username, path=file_path )
      request.session['flash'] = { 'type': 'success', 'message': 'Template file was loaded.'}
    except Exception as e:
      import sys, traceback
      traceback.print_exc(file=sys.stdout)
      request.session['flash'] = { 'type': 'error', 'message': 'There was an error loading Template file. Message: %s' % str(e) }
      return redirect('/webtoaster/templates/new')
    return redirect('/webtoaster/templates')

  t = loader.get_template('templates/new.html')
  c = RequestContext(request, {
    })
  c.update(csrf(request))
  return HttpResponse(t.render(c))

def destroy(request, template_id):
  template_id = int( template_id )
  try:    
    response = Templates.destroy( template_id )
    request.session['flash'] = { 'type': 'success', 'message': 'Template file was deleted.'}
  except Exception as e:
    request.session['flash'] = { 'type': 'error', 'message': 'Toaster produced an error while deleting Template file. Message: %s' % str(e) }

  if request.GET.get('after'):
    redirect_url = request.GET.get('after')
  else:
    redirect_url = '/webtoaster/templates'

  return redirect( redirect_url )