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

#Debugging flags
colour = True # Colourise terminal output
verbosity = 0 # Print extra output
helpful_debugging = True # Add info about file/line when debugging
excessive_verbosity = True # Add info about file/line when being verbose
import debug
