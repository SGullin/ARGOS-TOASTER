from subprocess import call
import subprocess
import  os, sys
import sys


from toolkit.pulsars.show_pulsars import get_pulsarinfo
from toolkit.pulsars.add_pulsar import add_pulsar

from toolkit.parfiles.get_parfile_id import get_parfiles
from toolkit.parfiles.load_parfile import load_parfile
from toolkit.parfiles.remove_parfile import remove_parfile_entry

from toolkit.timfiles.describe_timfiles import get_timfiles

from toolkit.templates.get_template_id import get_templates, add_arguments
from toolkit.templates.load_template import load_template

from toolkit.rawfiles.load_rawfile import load_rawfile


from add_telescope import add_telescope
import utils


from add_user import add_new_user
#import errors


class Pulsars:
  @classmethod
  def show(cls, pulsar_ids=None):

    from utils import get_pulsaralias_cache, get_pulsarid_cache, get_pulsarname_cache
    get_pulsaralias_cache(update=True)
    get_pulsarid_cache(update=True)
    get_pulsarname_cache(update=True)
    pulsars = cls.init_pulsars(get_pulsarinfo(pulsar_ids=pulsar_ids))
    return pulsars

  @classmethod
  def init_pulsars(cls, pulsars_dict):
    pulsars = list()
    for key in pulsars_dict.keys():
      pulsars.append( Pulsar( key, pulsars_dict[key] ) ) 
    return pulsars

  @classmethod
  def add(cls, name, aliases=list()):
    from utils import get_pulsaralias_cache, get_pulsarid_cache
    get_pulsaralias_cache(update=True)
    get_pulsarid_cache(update=True)
    response = add_pulsar(name, aliases)
    return response

  @classmethod
  def execute(cls,command="show", options=""):
    command_str = "%s/pulsar.py %s %s" % ( TOASTER_PYTHON_PATH, command, options )
    print command_str
    command = subprocess.Popen( "/bin/bash -l -c '"+command_str+"'" , stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    out,err = command.communicate()
    return { 'out': out.replace("\n","<br/>"), 'err': err }


class Telescopes:
  @classmethod
  def show(cls, pulsar_ids=None):
    raise Exception("Telescopes.show() needs to be defined in: %s" % __file__)
    return None

  @classmethod
  def init_pulsars(cls, pulsars_dict):
    pulsars = list()
    for key in pulsars_dict.keys():
      pulsars.append( Pulsar( key, pulsars_dict[key] ) ) 
    return pulsars

  @classmethod
  def add(cls, name, itrf_x, itrf_y, itrf_z, abbrev, code, aliases=[]):
    from utils import get_pulsaralias_cache, get_pulsarid_cache
    get_pulsaralias_cache(update=True)
    get_pulsarid_cache(update=True)
    response = add_telescope(name=name, \
                            itrf_x=itrf_x, \
                            itrf_y=itrf_y, \
                            itrf_z=itrf_z, \
                            abbrev=abbrev, \
                            code=code, \
                            aliases=aliases)
    return response


class Parfiles:
  @classmethod
  def show(cls, parfile_id=None):
    if parfile_id == None:
      parfiles = get_parfiles('%')
    else:
      parfiles = get_parfiles('%', parid=parfile_id)
    print parfiles
    return parfiles

  @classmethod
  def upload(cls, username, path, is_master=False):
    def my_function():
      return username;
    utils.get_current_username = my_function
    response=load_parfile(fn=path)
    return response

  @classmethod
  def destroy(cls, parfile_id):
    def my_function():
      return username;
    utils.get_current_username = my_function
    response=remove_parfile_entry( parfile_id )
    return response


class Timfiles:
  @classmethod
  def show(cls, timfile_id=None):
    if timfile_id == None:
      timfiles = get_timfiles()
      print "here"
    else:
      timfiles = get_timfiles('%', parid=timfile_id)
    print timfiles
    return timfiles


class Templates:
  @classmethod
  def show(cls):
    parser = utils.DefaultArguments(description='DESCRIPTION')
    add_arguments(parser)
    args = parser.parse_args()
    templates = get_templates(args)
    print templates
    return templates

  @classmethod
  def upload(cls, username, path, is_master=False):
    def my_function():
      return username;
    utils.get_current_username = my_function
    response=load_template(fn=path, comments="", is_master=False)
    return response

  @classmethod
  def destroy(cls, parfile_id):
    def my_function():
      return username;
    utils.get_current_username = my_function
    response=remove_parfile_entry( parfile_id )
    return response

class RawFiles:
  @classmethod
  def show(cls):
    parser = utils.DefaultArguments(description='DESCRIPTION')
    add_arguments(parser)
    args = parser.parse_args()
    templates = get_templates(args)
    print templates
    return templates

  @classmethod
  def upload(cls, username, path):
    def my_function():
      return username;
    utils.get_current_username = my_function
    response=load_rawfile( fn=path )
    return response

  @classmethod
  def destroy(cls, parfile_id):
    def my_function():
      return username;
    utils.get_current_username = my_function
    response=remove_parfile_entry( parfile_id )
    return response


class User:
  @classmethod
  def create(cls, auth_user ):
    # def add_new_user(db, user_name, real_name, email_address, passwd_hash, \
    #                 is_active=True, is_admin=False):
    user_id = add_new_user( user_name=auth_user.username, real_name="%s %s" % (auth_user.first_name, auth_user.last_name), is_admin=auth_user.is_staff )
    auth_user.userprofile.toaster_user_id = user_id
    auth_user.userprofile.save()
    return user_id

class Pulsar:

  def __init__(self, id_in, toaster_dict):
    self.id = id_in
    self.name = toaster_dict['name']
    self.aliases = toaster_dict['aliases']
    self.raj = toaster_dict['raj']
    self.decj = toaster_dict['decj']
    self.period = toaster_dict['period']
    self.dm = toaster_dict['dm']
    self.observations = 0
    self.num_toas = toaster_dict['numtoas']
    self.curators = toaster_dict['curators']
    self.telescopes = toaster_dict['telescopes']

  def parfiles(self):
    parfiles = get_parfiles(self.name)
    return parfiles