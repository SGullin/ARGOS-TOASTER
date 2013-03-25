from django.conf import settings

CONSUMER_KEY_SIZE = getattr(settings, 'OAUTHCLIENT_CONSUMER_KEY_SIZE', 32)
CONSUMER_SECRET_SIZE = getattr(settings, 'OAUTHCLIENT_CONSUMER_SECRET_SIZE', 32)
REDIRECT_AFTER_LOGIN = getattr(settings, 'OAUTHCLIENT_REDIRECT_AFTER_LOGIN', 'app.controllers.home_controller.index')
REDIRECT_AFTER_LOGOUT = getattr(settings, 'OAUTHCLIENT_REDIRECT_AFTER_LOGOUT', None)
LOGIN_TEMPLATE = getattr(settings, 'OAUTHCLIENT_LOGIN_TEMPLATE', 'login.html')
LOGOUT_TEMPLATE = getattr(settings, 'OAUTHCLIENT_LOGOUT_TEMPLATE', 'logout.html')
ERROR_TEMPLATE = getattr(settings, 'OAUTHCLIENT_ERROR_TEMPLATE', 'error.html')
