from django.conf.urls.defaults import *

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Example:
    # (r'^webtoaster/', include('webtoaster.foo.urls')),

    #(r'^/?$','app.home_controller.index'),
    # Uncomment the admin/doc line below and add 'django.contrib.admindocs' 
    # to INSTALLED_APPS to enable admin documentation:
    # (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    (r'^admin/', include(admin.site.urls)),
    (r'^/?$','app.controllers.home_controller.index'),

    (r'^pulsars/add','app.controllers.pulsars_controller.add'),
    (r'^pulsars/(?P<pulsar_id>\d+)/?$','app.controllers.pulsars_controller.show'),    
    (r'^pulsars/(?P<pulsar_id>\d+)/master_parfile/?$','app.controllers.pulsars_controller.master_parfile'),
    (r'^pulsars/?$','app.controllers.pulsars_controller.index'),

    (r'^telescopes/new','app.controllers.telescopes_controller.new'),
    (r'^telescopes/(?P<telescope_id>\d+)/?$','app.controllers.telescopes_controller.show'),
    (r'^telescopes/?$','app.controllers.telescopes_controller.index'),

    (r'^parfiles/?$','app.controllers.parfiles_controller.index'),
    (r'^parfiles/(?P<parfile_id>\d+)/destroy/?$','app.controllers.parfiles_controller.destroy'),
    (r'^parfiles/new/?$','app.controllers.parfiles_controller.new'),
    (r'^parfiles/(?P<parfile_id>\d+)/download/?$','app.controllers.parfiles_controller.download'),

    (r'^templates/?$','app.controllers.templates_controller.index'),
    (r'^templates/(?P<parfile_id>\d+)/destroy/?$','app.controllers.templates_controller.destroy'),
    (r'^templates/new/?$','app.controllers.templates_controller.new'),

    (r'^rawfiles/?$','app.controllers.rawfiles_controller.index'),
    (r'^rawfiles/(?P<rawfile_id>\d+)/destroy/?$','app.controllers.rawfiles_controller.destroy'),
    (r'^rawfiles/new/?$','app.controllers.rawfiles_controller.new'),


    (r'^help/','app.controllers.help_controller.index'),
    (r'^about/','app.controllers.about_controller.index'),
    (r'^oauth/', include('oauthclient.urls', namespace='oauth',
            app_name='app'), {'identifier': 'default'}),
    (r'^login','app.controllers.users_controller.authentication'),
    (r'^logout','app.controllers.users_controller.destroy_session'),
    (r'^profile','app.controllers.users_controller.profile'),

)
