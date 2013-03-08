#!/usr/local/PULSAR/py_env_2.6.5/bin/python

import  os, sys
#Calculate the path based on the location of the WSGI script.
apache_configuration= os.path.dirname(__file__)
project = os.path.dirname(apache_configuration)
workspace = os.path.dirname(project)
sys.path.append('/home/palfa/webtoaster')

sys.stdout = sys.stderr
os.environ['DJANGO_SETTINGS_MODULE'] = "settings"
os.environ['TOASTER_CFG'] = '/home/palfa/webtoaster-patrick/toaster/toaster.cfg'
os.environ['DJANGO_SETTINGS_MODULE'] = "production_settings"
import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()

import monitor
monitor.start(interval=1.0)
