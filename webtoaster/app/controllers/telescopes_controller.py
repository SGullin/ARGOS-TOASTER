from django.http import Http404
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import *
from django.template import Context, RequestContext
from django.template import loader
from app.models import *
from httplib import HTTPResponse
from lib.toaster import Telescopes
from django.core.context_processors import csrf
from django.conf import settings

from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.core.exceptions import ObjectDoesNotExist
import oauth2

from django.contrib.auth.decorators import login_required
from django import forms

from django.core.paginator import Paginator


class TelescopeForm(forms.Form):
  name = forms.CharField(required=True, max_length=64)
  itrf_x = forms.FloatField(required=True)
  itrf_y = forms.FloatField(required=True)
  itrf_z = forms.FloatField(required=True)
  abbrev = forms.CharField(required=True, max_length=16)
  code = forms.CharField(required=True, max_length=2)
  latitude = forms.FloatField(required=False)
  longitude = forms.FloatField(required=False)
  datum = forms.CharField(required=False, max_length=64)

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


  per_page = 2

  telescopes = Telescope.objects.all()


  # if order == 'asc':
  #   pulsars_sorted = list(reversed(pulsars_sorted))
  # elif order == 'desc':
  #   pass


  telescopes_paged = Paginator(telescopes, per_page)
  telescopes_current_page = telescopes_paged.page(page)

  t = loader.get_template('telescopes/index.html')
  c = RequestContext(request, {
    'telescopes': telescopes_current_page,
    'page_range': telescopes_paged.page_range,
    'sort_by': sort_by,
    'order': order
    })
  return HttpResponse(t.render(c))

def new(request):

  if request.method == 'GET':    
    form = TelescopeForm()
    aliases = list()
  elif request.method == 'POST':
    aliases = request.POST.getlist('aliases[]')
    form = TelescopeForm(request.POST)
    if form.is_valid():
      new_telescope_name = form.cleaned_data['name']
      new_aliases = aliases
      try:
        response = Telescopes.add(name=form.cleaned_data['name'], \
                                 itrf_x=form.cleaned_data['itrf_x'], \
                                 itrf_y=form.cleaned_data['itrf_y'], \
                                 itrf_z=form.cleaned_data['itrf_z'], \
                                 abbrev=form.cleaned_data['abbrev'], \
                                 code=form.cleaned_data['code'], \
                                 aliases=aliases)
        request.session['flash'] = { 'type': 'success', 'message': 'Telescope was succesfully added with iD: %i' % response }
        return redirect("/webtoaster/telescopes/%i/" % response)
      except Exception, e:
        request.session['flash'] = { 'type': 'error', 'message': 'Toaster produced an error: %s' %  str(e)}      
    else:
      request.session['flash'] = { 'type': 'error', 'message': 'Please verify your form' }

  t = loader.get_template('telescopes/new.html')
  c = RequestContext(request, {
    'form': form,
    'aliases': aliases,
    })
  return HttpResponse(t.render(c))


def show(request, telescope_id):
  telescope_id = int( pulsar_id )
  telescope = Telescopes.show( telescopes_ids=[pulsar_id])[0]
  t = loader.get_template('pulsars/show.html')
  c = RequestContext(request, {
    'pulsar': pulsar,
    })
  return HttpResponse(t.render(c))

def get_columns(pulsars):
  key = pulsars.keys()[0]
  return pulsars[key].keys()