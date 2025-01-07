#!/usr/bin/env python
import os


class ToasterConfigs(dict):
    def __init__(self):
        super(ToasterConfigs, self).__init__()
        #self['toaster_dir'] = os.path.split(os.path.abspath(__file__))[0]
        #fn = os.path.join(self.toaster_dir, "default.cfg")
        #self.load_config_file(fn)
        self.loaded_configs = False
        
    def load_configs(self, cfg_files=None):
        """Load configuration files. If no configuration files are provided
            read list of files from TOASTER_CFG environment variable.

            Inputs:
                cfg_files: A list of configuration files to load.
                    Files listed earlier in the list (ie lower indices)
                    supersede files provided later.
                    (Default: Read list of config files from "TOASTER_CFG"
                    environment variable.)

            Outputs:
                None
        """
        if cfg_files is None:
            cfg_files = os.environ.get("TOASTER_CFG", "").split(':')
        for cfg_file in reversed(cfg_files):
            self.load_config_file(cfg_file)

        if not self.loaded_configs:
            raise ValueError("No configuration files loaded. "
                             "Please set environment variable 'TOASTER_CFG' and "
                             "ensure at least one file listed is accessible.")
        
    def __getattr__(self, key):
        from toaster import errors
        if key not in self:
            raise errors.NoConfigError("There is no config param called '%s' "
                                       "defined!" % key)
        return self[key]

    def __str__(self):
        lines = []
        for key in sorted(self.keys()):
            lines.append("%s: %r" % (key, self[key]))
        return "\n".join(lines)
    
    def load_config_file(self, fn):
        #print("Loading configs from %s" % fn)
        if os.path.isfile(fn):
            if not fn.endswith('.cfg'):
                raise ValueError("TOASTER configuration files must "
                                 "end with the extension '.cfg'.")
            exec(open(fn).read(), {}, self)
            self.loaded_configs = True


cfg = ToasterConfigs()
cfg.load_configs()


def main():
    print(cfg)


if __name__ == '__main__':
    from toaster import utils
    parser = utils.DefaultArguments(description="Print configurations to terminal.")
    args = parser.parse_args()
    main()
