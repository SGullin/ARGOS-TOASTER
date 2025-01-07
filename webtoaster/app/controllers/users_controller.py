from django.http import Http404
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import *
from django.template import Context, RequestContext
from django.template import loader
from app.models import *
from httplib import HTTPResponse
from django.core.context_processors import csrf
from django.conf import settings

from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.core.exceptions import ObjectDoesNotExist
import oauth2

from django.contrib.auth.decorators import login_required
from django import forms

class ProfileForm(forms.Form):
  password   = forms.CharField(required=False)
  first_name = forms.CharField()
  last_name  = forms.CharField()
  email      = forms.EmailField()


#/login
def authentication(request):
  next_url = request.GET.get('next')
  user= None
  if request.method == 'POST':
    username = request.POST.get('userlogin')
    password = request.POST.get('userpassword')
    user = authenticate( username= username, password= password ) 
    if user is not None:
        if user.is_active:
            login(request,user)
            redirect_to_url = settings.ROOT_URL
            if next_url != None:
              redirect_to_url = next_url
            verify_or_create_toaster_user( user )
            return redirect(redirect_to_url)
        else:
            message = "Account is Disabled"
    else:
        message = "Account is Invalid"
  elif 'username' in request.GET: #username was provided so assuming Authentication with CyberSKA
    if  ('access_token' and 'secret') in request.GET: 
      #If CyberSKA user already acquired the tokens and CyberSKA is passing it to us
      user= login_user_with_cyberska( request )
    if user == None:
      return redirect("%s/oauth/request_token/" % settings.ROOT_URL)

  t = loader.get_template('users/login.html')
 
  c = RequestContext(request, {
    'welcome_message': 'Welcome to Web-Toaster, friend!',
    'cyberska_app_link': settings.CYBERSKA_APP_URL,
  })
  c.update(csrf(request))

  
  if request.user.is_authenticated():
    return redirect(settings.ROOT_URL)

  return HttpResponse(t.render(c))

#/logout
def destroy_session(request):
  logout(request)
  return redirect(settings.ROOT_URL)

def login_user_with_cyberska(request):  
  print(dict(request.session))
  token = request.GET.get('access_token')
  secret = request.GET.get('secret')
  username = request.GET.get('username')

  try:
    user = User.objects.get( username= username, userprofile__oauth_token= token, userprofile__oauth_token_secret= secret )
    user.backend = 'django.contrib.auth.backends.ModelBackend'
    login( request, user)
  except ObjectDoesNotExist as e:
    user = None
  # token_client = oauth2.Token( token, secret )
  return user

@login_required
def profile(request):

  if request.method == 'POST':
    profile_form = ProfileForm(request.POST)
    if not profile_form.is_valid():
      request.session['flash'] = { 'type': 'error', 'message': 'Please check the form for errors.'}
    else:
      user = request.user
      user.first_name = profile_form.cleaned_data['first_name']
      user.last_name = profile_form.cleaned_data['last_name']
      user.email = profile_form.cleaned_data['email']
      print(profile_form.cleaned_data)
      if profile_form.cleaned_data['password'] != u'':
        user.set_password( profile_form.cleaned_data['password'] )
      user.save()
      request.session['flash'] = { 'type': 'success', 'message': 'Your profile was successfully updated.'}
  else:
    user = request.user
    form_dict = {'first_name': user.first_name,
                'last_name': user.last_name,
                'email': user.email }
    print(form_dict)
    profile_form = ProfileForm(form_dict)

  t = loader.get_template('users/profile.html')
  c = RequestContext(request, {
    'welcome_message': 'Welcome to Web-Toaster, friend!',
    'cyberska_app_link': settings.CYBERSKA_APP_URL,
    'form': profile_form
  })
  c.update(csrf(request))
  return HttpResponse(t.render(c))

def update(request):
  if not request.user.is_authenticated():
    return redirect(settings.ROOT_URL +'user/profile')

def verify_or_create_toaster_user( user ):
    if len( user.userprofile_set.all() )==0:
      toaster_user = ToasterUser.objects.get_or_create(user_name=user.username)[0]
      toaster_user.real_name = "%s %s" % ( user.first_name, user.last_name )
      toaster_user.email_address = user.email
      toaster_user.active = user.is_active
      toaster_user.admin = user.is_staff
      user.userprofile.toaster_user = toaster_user
      toaster_user.save()
      user.userprofile.save()
    

