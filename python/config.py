import os.path
##############################################################################
# CONFIG PARAMS
#############################################################################

pipe_name = "epta_pipeline"
version = 0.1

#Database parameters
dbhost = "eptadata.jb.man.ac.uk"
dbname = "epta"
dbuser = "epta"
dbpass = "psr1937"

#Python version to use
python = "/usr/bin/python"

data_archive_location = '/raid1/database/data/'

# Location of software packages
epta_pipeline_dir = os.path.split(os.path.abspath(__file__))[0]
psrchive_dir = "/raid1/home/bassa/linux/src/psrchive/"
tempo2_dir = "/raid1/home/bassa/linux/src/tempo2/"

#Debugging flags
colour = True # Colourise terminal output
verbosity = 1 # Print extra output
helpful_debugging = True # Add info about file/line when debugging
excessive_verbosity = True # Add info about file/line when being verbose
import debug
