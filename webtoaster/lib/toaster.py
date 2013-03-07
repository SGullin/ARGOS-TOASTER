from subprocess import call
import subprocess
import  os, sys
import sys


from toolkit.pulsars.show_pulsars import get_pulsarinfo
from toolkit.pulsars.add_pulsar import add_pulsar
#import errors


class Pulsars:
  @classmethod
  def show(cls):
    pulsars = cls.init_pulsars(get_pulsarinfo())
    return pulsars

  @classmethod
  def init_pulsars(cls, pulsars_dict):
    pulsars = list()
    for key in pulsars_dict.keys():
      pulsars.append( Pulsar( key, pulsars_dict[key] ) ) 
    return pulsars

  @classmethod
  def add(cls, name, aliases=list()):
    response = add_pulsar(name, aliases)
    return response

  @classmethod
  def execute(cls,command="show", options=""):
    command_str = "%s/pulsar.py %s %s" % ( TOASTER_PYTHON_PATH, command, options )
    print command_str
    command = subprocess.Popen( "/bin/bash -l -c '"+command_str+"'" , stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    out,err = command.communicate()
    return { 'out': out.replace("\n","<br/>"), 'err': err }


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