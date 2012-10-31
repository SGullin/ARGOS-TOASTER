import os.path
import glob

import debug
import errors
import utils

class ToasterConfigs(dict):
    def __init__(self):
        # Load default configurations
        self['toaster_dir'] = os.path.split(os.path.abspath(__file__))[0]
        fn = os.path.join(self.toaster_dir, "default.cfg") 
        self.load_configs(fn)
        
        # Load configurations from TOASTER_CFG environment variable
        cfg_files = os.getenv("TOASTER_CFG", "").split(':')
        for cfg_file in reversed(cfg_files):
            self.load_configs(cfg_file)
        
    def __getattr__(self, key):
        if key not in self:
            raise errors.NoConfigError("There is no config param called '%s' " \
                                        "defined!" % key) 
        return self[key]

    def __str__(self):
        lines = []
        for key in sorted(self.keys()):
            lines.append("%s: %r" % (key, self[key]))
        return "\n".join(lines)
    
    def load_configs(self, fn):
        if os.path.isfile(fn):
            if not fn.endswith('.cfg'):
                raise ValueError("TOASTER configuration files must " \
                                        "end with the extention '.cfg'.")
            execfile(fn, {}, self)


cfg = ToasterConfigs()
