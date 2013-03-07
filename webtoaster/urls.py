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
    (r'^pulsars/','app.controllers.pulsars_controller.index'),
    (r'^help/','app.controllers.help_controller.index'),
    (r'^about/','app.controllers.about_controller.index'),
    (r'^oauth/', include('oauthclient.urls', namespace='oauth',
            app_name='app'), {'identifier': 'default'}),
    (r'^login','app.controllers.users_controller.authentication'),
    (r'^logout','app.controllers.users_controller.destroy_session'),
    (r'^profile','app.controllers.users_controller.profile'),

)
