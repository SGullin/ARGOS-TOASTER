#!/usr/bin/python2.6

import  os, sys
#Calculate the path based on the location of the WSGI script.
apache_configuration= os.path.dirname(__file__)
project = os.path.dirname(apache_configuration)
workspace = os.path.dirname(project)
#sys.path.append(workspace) 
#sys.path.append('/exports/WWW/people/snipka/')
sys.path.append('/home/snip3/dev/pythonapps/webtoaster')
#sys.path.append('/home/pulsar/public_html/hattak/')
#sys.path.insert(0, '/home/pulsar/public_html/hattak')

#import settings

#import django.core.management
#django.core.management.setup_environ(settings)
#utility = django.core.management.ManagementUtility()
#command = utility.fetch_command('runserver')

#command.validate()

#import django.conf
#import django.utils

#django.utils.translation.activate(django.conf.settings.LANGUAGE_CODE)

#import django.core.handlers.wsgi

#application = django.core.handlers.wsgi.WSGIHandler()

#Add the path to 3rd party django application and to django itself.
#sys.path.append('C:\\yml\\_myScript_\\dj_things\\web_development\\svn_views\\django_src\\trunk')
#sys.path.append('C:\\yml\\_myScript_\\dj_things\\web_development\\svn_views\\django-registration')

sys.stdout = sys.stderr
os.environ['DJANGO_SETTINGS_MODULE'] = "settings"
import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()

import monitor
monitor.start(interval=1.0)
