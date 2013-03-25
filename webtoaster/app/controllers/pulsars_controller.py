from django.http import Http404
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import *
from django.template import Context, RequestContext
from django.template import loader
from app.models import *
from httplib import HTTPResponse
from lib.toaster import Pulsars
from django.core.context_processors import csrf
from django.conf import settings

from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.core.exceptions import ObjectDoesNotExist
import oauth2

from django.contrib.auth.decorators import login_required
from django import forms

from django.core.paginator import Paginator


class PulsarForm(forms.Form):
  name = forms.CharField()

def index(request):
  page = request.GET.get('page')
  if page == None:
    page = 1
  else:
    page = int( page )

  sort_by = request.GET.get('sort_by')
  order = request.GET.get('order')

  if sort_by == None:
    sort_by = 'id'

  if order == None:
    order == 'desc'


  per_page = 10

  pulsars = Pulsars.show()

  def get_sort_method(pulsar):
    return getattr(pulsar, sort_by)

  pulsars_sorted = sorted(pulsars, key=lambda pulsar: get_sort_method(pulsar) )

  if order == 'asc':
    pulsars_sorted = list(reversed(pulsars_sorted))
  elif order == 'desc':
    pass


  pulsars_paged = Paginator(pulsars_sorted, per_page)
  pulsars_current_page = pulsars_paged.page(page)

  t = loader.get_template('pulsars/index.html')
  c = RequestContext(request, {
    'welcome_message': 'Welcome to Web-Toaster, friend!',
    'pulsars': pulsars_current_page,
    'page_range': pulsars_paged.page_range,
    'sort_by': sort_by,
    'order': order
    })
  return HttpResponse(t.render(c))

def add(request):

  if request.method == 'GET':    
    form = PulsarForm()
    aliases = list()
  elif request.method == 'POST':
    aliases = request.POST.getlist('aliases[]')
    form = PulsarForm(request.POST)
    if form.is_valid():
      new_pulsar_name = form.cleaned_data['name']
      new_aliases = aliases
      try:
        response = Pulsars.add(new_pulsar_name, aliases)
        request.session['flash'] = { 'type': 'success', 'message': 'Pulsar was succesfully added with iD: %i' % response }
        return redirect("/webtoaster/pulsars/%i/" % response)
      except Exception, e:
        request.session['flash'] = { 'type': 'error', 'message': 'Toaster produced an error: %s' %  str(e)}      
    else:
      request.session['flash'] = { 'type': 'error', 'message': 'Please verify your form' }

  t = loader.get_template('pulsars/add.html')
  c = RequestContext(request, {
    'welcome_message': 'Welcome to Web-Toaster, friend!',
    'form': form,
    'aliases': aliases,
    })
  return HttpResponse(t.render(c))


def show(request, pulsar_id):
  pulsar_id = int( pulsar_id )
  pulsar = Pulsars.show( pulsar_ids=[pulsar_id])[0]
  t = loader.get_template('pulsars/show.html')
  c = RequestContext(request, {
    'pulsar': pulsar,
    })
  return HttpResponse(t.render(c))

def master_parfile(request, pulsar_id):
  pulsar_id = int( pulsar_id )
  pulsar = Pulsars.show( pulsar_ids=[pulsar_id])[0]
  parfiles = pulsar.parfiles()
  if parfiles != []:
    parfile = parfiles[0]
  else:
    parfile=None
  print parfile.__class__
  t = loader.get_template('pulsars/master_parfile.html')
  c = RequestContext(request, {
    'parfile': parfile,
    'pulsar': pulsar,
    })
  return HttpResponse(t.render(c))

def get_columns(pulsars):
  key = pulsars.keys()[0]
  return pulsars[key].keys()