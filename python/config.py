import os.path
##############################################################################
# CONFIG PARAMS
#############################################################################

pipe_name = "epta_pipeline"
version = 0.1

# Database parameters
dburl = "sqlite:///test.db"
dbhost = "eptadata.jb.man.ac.uk"
dbname = "epta"
dbuser = "epta"
dbpass = "psr1937"

#Python version to use
python = "/usr/bin/python"

# TOA generation configurations
toa_fitting_method = "FDM" # see PSRCHIVE program 'pat' for 
                           # a list of valid fitting methods
                           # NOTE: The Goodness-of-Fit is only 
                           #        available with 'FDM' method. 


data_archive_location = '/media/Data/toaster/archive/'
diagnostics_location = os.path.join(data_archive_location, "diagnostics")

# Location of software packages
epta_pipeline_dir = os.path.split(os.path.abspath(__file__))[0]
psrchive_dir = "/home/plazar/packages/psrchive-git/"
tempo2_dir = "/home/plazar/research/pulsar-code/linux/src/tempo2/"

# Should we do archiving?
archive = False

# Debugging flags
colour = True # Colourise terminal output
verbosity = 1 # Print extra output
helpful_debugging = True # Add info about file/line when debugging
excessive_verbosity = True # Add info about file/line when being verbose
import debug
