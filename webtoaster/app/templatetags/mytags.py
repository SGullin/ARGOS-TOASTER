from django import template
from lib import approx_harm
from django.conf import settings

register = template.Library()

class MyRootUrl(template.Node):
    def __init__(self):
        self.tmp = ''
    def render(self, context):
#  return '/palfa3-apps/'
         return settings.ROOT_URL
        
def root_url(blah, blah1):
    return MyRootUrl()
register.tag('root_url',root_url)