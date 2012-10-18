import os.path
##############################################################################
# CONFIG PARAMS
#############################################################################

pipe_name = "epta_pipeline"
version = 0.1

# Database parameters
dburl = "sqlite:///test.db"

#Python version to use
python = "/usr/bin/python"

# TOA generation configurations
toa_fitting_method = "FDM" # see PSRCHIVE program 'pat' for 
                           # a list of valid fitting methods
                           # NOTE: The Goodness-of-Fit is only 
                           #        available with 'FDM' method. 


data_archive_location = '/aux/pc20237a/plazar/timing/toaster/archive/'
diagnostics_location = os.path.join(data_archive_location, "diagnostics")

# Location of software packages
epta_pipeline_dir = os.path.split(os.path.abspath(__file__))[0]
psrchive_dir = "/aux/pc20237a/soft/psrchive/"
tempo2_dir = "/home/plazar/research/pulsar-code/linux/src/tempo2/"

############
# Archiving
############
# Should we do archiving?
archive = True
# Should we move the files when archive
# (if False, copy files - leave originals in place)
move_on_archive = False

# Base dir for creating temporary files
# Set to None to use a system-default location
base_tmp_dir = "/dev/shm/"

# Debugging flags
colour = True # Colourise terminal output
verbosity = 1 # Print extra output
helpful_debugging = True # Add info about file/line when debugging
excessive_verbosity = True # Add info about file/line when being verbose
import debug
