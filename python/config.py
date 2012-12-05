#!/usr/bin/env python
import os.path
import glob

import debug
import errors
import utils

class ToasterConfigs(dict):
    def __init__(self):
        #self['toaster_dir'] = os.path.split(os.path.abspath(__file__))[0]
        #fn = os.path.join(self.toaster_dir, "default.cfg") 
        #self.load_config_file(fn)
        self.loaded_configs = False
        
    def load_configs(self):
        """Load configurations from TOASTER_CFG environment variable
        """
        cfg_files = os.getenv("TOASTER_CFG", "").split(':')
        for cfg_file in reversed(cfg_files):
            self.load_config_file(cfg_file)

        if not self.loaded_configs:
            raise errors.FatalToasterError("No configuration files loaded. " \
                        "Please set environment variable 'TOASTER_CFG' and " \
                        "ensure at least one file listed is accessible.")
        
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
    
    def load_config_file(self, fn):
        #print "Loading configs from %s" % fn
        if os.path.isfile(fn):
            if not fn.endswith('.cfg'):
                raise ValueError("TOASTER configuration files must " \
                                        "end with the extention '.cfg'.")
            execfile(fn, {}, self)
            self.loaded_configs = True


cfg = ToasterConfigs()
cfg.load_configs()


def main():
    print cfg


if __name__=='__main__':
    parser = utils.DefaultArguments(\
                description="Print configurations to terminal.")
    args = parser.parse_args()
    main()
