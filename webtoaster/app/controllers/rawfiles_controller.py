from django.http import Http404
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import *
from django.template import Context, RequestContext
from django.template import loader
from app.models import *
from httplib import HTTPResponse
from lib.toaster import Pulsars
from lib.toaster import Templates, RawFiles

from django.core.context_processors import csrf
from django.conf import settings

from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.core.exceptions import ObjectDoesNotExist
import oauth2

from django.contrib.auth.decorators import login_required
from django import forms

from django.core.paginator import Paginator


def index(request):
  rawfiles = RawFiles.show()

  t = loader.get_template('rawfiles/index.html')
  c = RequestContext(request, {
    'rawfiles': rawfiles,
    })
  return HttpResponse(t.render(c))

def new(request):
  import os
  if request.method == 'POST' and request.FILES.get('rawfile'):
    try:
      uf = request.FILES['rawfile']
      temp_path = settings.TEMP_DIR
      fn = uf.name
      file_path = os.path.join( temp_path, fn )
      open( file_path, 'w' ).write( uf.read() )
      load_status = RawFiles.upload( username=request.user.username, path=file_path )
      request.session['flash'] = { 'type': 'success', 'message': 'Raw file was loaded.'}
    except Exception as e:
      import sys, traceback
      traceback.print_exc(file=sys.stdout)
      request.session['flash'] = { 'type': 'error', 'message': 'There was an error loading Raw file. Message: %s' % str(e) }
      return redirect('/webtoaster/rawfiles/new')
    return redirect('/webtoaster/rawfiles')

  t = loader.get_template('rawfiles/new.html')
  c = RequestContext(request, {
    })
  c.update(csrf(request))
  return HttpResponse(t.render(c))

def destroy(request, rawfile_id):
  rawfile_id = int( rawfile_id )
  
  try:
    response = Templates.destroy( rawfile_id )
    request.session['flash'] = { 'type': 'success', 'message': 'Raw file was deleted.'}
  except Exception as e:
    request.session['flash'] = { 'type': 'error', 'message': 'Toaster produced an error while deleting Raw file. Message: %s' % str(e) }

  if request.GET.get('after'):
    redirect_url = request.GET.get('after')
  else:
    redirect_url = '/webtoaster/rawfiles'

  return redirect( redirect_url )