# django imports
from django.shortcuts import render_to_response, redirect
from django.contrib.sites.models import Site
from django.core.urlresolvers import reverse

# oauth imports
import oauth2
import urlparse

#oauthclient import
from models import ConsumerToken, OAuthServer
from oauthclient import settings
from app.models import *
from django.contrib.auth import authenticate, login, logout

from django.contrib.auth.models import User

"""These views are a generic way to do a three legged authentication with OAuth. 

You can find more information on three legged authentication on the OAuth
website: http://oauth.net/core/diagram.png

"""

def get_request_token(request, identifier='default'):
    """First and second step of the three-legged OAuth flow:
    
    Request a request token to the OAuth server, and redirect the user on the
    OAuth server, to authorize user access, aka steps A, B and C.
    
    Once this done, the server redirect the user on the access_token_ready
    view.
    
    """
    token = ConsumerToken.objects.get(identifier=identifier)
    client = oauth2.Client(token.get_consumer())
    resp, content = client.request(token.server.get_request_token_url(), "POST")

    if resp['status'] != '200':
        raise Exception("Invalid response %s. Reason: %s" % (resp['status'], content) )    

    request_token = dict(urlparse.parse_qsl(content))
    if not ('oauth_token' and 'oauth_token_secret' in request_token):
        raise Exception("Invalid response: oauth_token and oauth_token_secret have to be present in the OAuth server response. Got: %s" % content)
    
    # store information in session
    request.session[identifier + '_request_token'] = request_token['oauth_token']
    request.session[identifier + '_request_token_secret'] = request_token['oauth_token_secret']
    
    #redirect the user to the authentication page
    callback_url = 'http://%s%s' % (
        Site.objects.get_current().domain, 
        reverse('oauth:access_token_ready'))
    
    redirect_url = "%s?oauth_token=%s&oauth_callback=%s" % (
        token.server.get_authorize_url(), 
        request_token['oauth_token'], 
        callback_url)

    if 'next' in request.GET:
        request.session['next'] = request.GET['next']
    request.session.save()
    return redirect(redirect_url)
    
def access_token_ready(request, identifier='default'):
    """Last step of the OAuth three-legged flow.

    The user is redirected here once he allowed (or not) the application to 
    access private informations, aka steps D, E and F.
    
    Echange a valid request token against a valid access token. If a valid 
    access token is given, store it in session.
    
    """

    print "access_token_ready"
    if not (identifier+'_request_token' and identifier+'_request_token_secret'
            in request.session):
        raise Exception('%s_request_token and %s_request_token_secret are not' \
            'present in session.' % (identifier, identifier))
    
    if ('error' in request.GET):
        return render_to_response(settings.ERROR_TEMPLATE, {
            'error':request.GET['error']
        })
    
    #if not 'oauth_verifier' in request.GET:
    #    raise Exception('oauth_verifier must be present in request.GET')
    
    token = ConsumerToken.objects.get(identifier=identifier)

    # Echange the request token against a access token.
    request_token = oauth2.Token(request.session[identifier + '_request_token'],
        request.session[identifier + '_request_token_secret'])
    #request_token.set_verifier(request.GET['oauth_verifier'])
    client = oauth2.Client(token.get_consumer(), request_token)
    resp, content = client.request(token.server.get_access_token_url() , "POST")
    access_token = dict(urlparse.parse_qsl(content))
    
    # test if access token is valid. 
    if not ('oauth_token' and 'oauth_token_secret' in access_token):
        raise Exception('oauth_token and oauth_token_secret must be present in the OAuth server response')
    print "Access Token:"
    print access_token
    request.session[identifier + '_oauth_token'] = access_token['oauth_token']
    request.session[identifier + '_oauth_token_secret'] = access_token['oauth_token_secret']
    request.session[identifier + '_username'] = access_token['username']
    request.session[identifier + '_token'] = access_token

    user = User.objects.get_or_create( username= access_token['loginname'] )[0]

    username = access_token['username']
    if len(username.split()) >= 2:
        first_name = username.split()[0]
        last_name = username.split()[1]
    else:
        first_name = username
        last_name = ''


    user.first_name = first_name
    user.last_name = last_name
    user.email = access_token['email']
    user.save()

    toaster_user = ToasterUser.objects.get_or_create(user_name=user.username)[0]
    toaster_user.real_name = username
    toaster_user.email_address = user.email
    toaster_user.save()

    user_profile = UserProfile.objects.get_or_create( user=user, toaster_user=toaster_user )[0]
    user_profile.oauth_token= access_token['oauth_token']
    user_profile.oauth_token_secret= access_token['oauth_token_secret']
    #user_profile.toaster_user = toaster_user
    user_profile.save()

    user.backend = 'django.contrib.auth.backends.ModelBackend'
    login( request, user)

    if 'next' in request.session:
        return redirect(request.session['next'])
    if settings.REDIRECT_AFTER_LOGIN == None:
        return render_to_response(settings.LOGIN_TEMPLATE)
    return redirect(settings.REDIRECT_AFTER_LOGIN)
    
def logout(request, identifier='default'):
    """Destruct the active session oauth related keys.
    
    """
    for key in ('oauth_token', 'oauth_token_secret', 
        'request_token', 'request_token_secret'):
        if identifier + '_' + key in request.session:
            del request.session[identifier + '_' + key]
    request.session.save()
    if settings.REDIRECT_AFTER_LOGOUT == None:
        return render_to_response(settings.LOGOUT_TEMPLATE)
    return redirect(settings.REDIRECT_AFTER_LOGOUT)
