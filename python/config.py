import os.path
import glob

cfg_files = glob.glob("*.cfg")
if len(cfg_files) == 0:
    cfg_file = os.path.join(os.path.split(__file__)[0], "default.cfg") 
else:
    cfg_file = cfg_files[0]
execfile(cfg_file, {}, locals())

diagnostics_location = os.path.join(data_archive_location, "diagnostics")
toaster_dir = os.path.split(os.path.abspath(__file__))[0]

import debug
