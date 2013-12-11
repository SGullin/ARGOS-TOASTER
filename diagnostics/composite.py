import tempfile
import os
import shutil

from toaster import utils
from toaster.utils import notify
from toaster.utils import datafile 
import base

class CompositePlotDiagnostic(base.PlotDiagnostic):
    name = 'Composite'
    description = "A composite plot including a profile, " \
                    "a time vs. phase plot, a freq vs. phase " \
                    "plot, and some text information. NOTE: " \
                    "if the raw file is fully scrunched along " \
                    "an axis the relevent plot will not be shown."
    def _compute(self):
        notify.print_info("Creating composite summary plot for %s" % self.fn, 3)
        handle, tmpfn = tempfile.mkstemp(suffix=".png")
        os.close(handle)
        params = datafile.prep_file(self.fn)
        
        if (params['nsub'] > 1) and (params['nchan'] > 1):
            self.__plot_all(tmpfn, params)
        elif (params['nsub'] > 1) and (params['nchan'] == 1):
            self.__plot_nofreq(tmpfn, params)
        elif (params['nsub'] == 1) and (params['nchan'] > 1):
            self.__plot_notime(tmpfn, params)
        elif  (params['nsub'] == 1) and (params['nchan'] == 1):
            self.__plot_profonly(tmpfn, params)
        else:
            raise errors.FileError("Not sure how to plot diagnostic for file. " \
                                    "(nsub: %d; nchan: %d)" % \
                                    (params['nsub'], params['nchan']))
        tmpdir = os.path.split(tmpfn)[0]
        archivefn = os.path.split(self.fn)[-1]
        pngfn = os.path.join(tmpdir, archivefn+".composite.png")
        shutil.move(tmpfn, pngfn) 
        return pngfn

    def __get_info(self, params):
        info = "above:l=%s\n" \
                       "%s    %s (%s)\n" \
                       "Length=%.1f s    BW=%.1f MHz\n" \
                       "N\\dbin\\u=$nbin    N\\dchan\\u=$nchan    N\\dsub\\u=$nsubint," \
               "above:off=3.5" % \
                        (os.path.split(self.fn)[-1], \
                         params['telescop'], params['rcvr'], \
                         params['backend'], params['length'], params['bw'])
        return info

    def __plot_profonly(self, tmpfn, params):
        info = self.__get_info(params)
        cmd = ["psrplot", "-O", "-j", "D", "-c", "above:c=,x:range=0:2", \
                self.fn, "-D", "%s/PNG" % tmpfn, \
                "-p", "flux", "-c", ":0:x:view=0.075:0.95," \
                                         "y:view=0.15:0.7," \
                                         "subint=I," \
                                         "chan=I," \
                                         "pol=I," \
                                         "below:l=," \
                                         "%s" % info]
        utils.execute(cmd)
        
    def __plot_nofreq(self, tmpfn, params):
        info = self.__get_info(params)
        cmd = ["psrplot", "-O", "-j", "D", "-c", "above:c=,x:range=0:2", \
                self.fn, "-D", "%s/PNG" % tmpfn, \
                "-p", "flux", "-c", ":0:x:view=0.075:0.95," \
                                        "y:view=0.5:0.7," \
                                        "subint=I," \
                                        "chan=I," \
                                        "pol=I," \
                                        "x:opt=BCTS," \
                                        "x:lab=," \
                                        "below:l=," \
                                        "%s" % info, \
                "-p", "time", "-c", ":1:x:view=0.075:0.95," \
                                        "y:view=0.15:0.5," \
                                        "chan=I," \
                                        "pol=I," \
                                        "cmap:map=plasma"]
        utils.execute(cmd)
        
    def __plot_notime(self, tmpfn, params):
        info = self.__get_info(params)
        cmd = ["psrplot", "-O", "-j", "D", "-c", "above:c=,x:range=0:2", \
                self.fn, "-D", "%s/PNG" % tmpfn, \
                "-p", "flux", "-c", ":0:x:view=0.075:0.95," \
                                       "y:view=0.5:0.7," \
                                       "subint=I," \
                                       "chan=I," \
                                       "pol=I," \
                                       "x:opt=BCTS," \
                                       "x:lab=," \
                                       "below:l=," \
                                       "%s" % info, \
                "-p", "freq", "-c", ":1:x:view=0.075:0.95," \
                                       "y:view=0.15:0.5," \
                                       "subint=I," \
                                       "pol=I," \
                                       "cmap:map=plasma"]
        utils.execute(cmd)
        
    def __plot_all(self, tmpfn, params):
        info = self.__get_info(params)
        cmd = ["psrplot", "-O", "-j", "D", "-c", "above:c=,x:range=0:2", \
                self.fn, "-D", "%s/PNG" % tmpfn, \
                "-p", "flux", "-c", ":0:x:view=0.575:0.95," \
                                       "y:view=0.7:0.9," \
                                       "subint=I," \
                                       "chan=I," \
                                       "pol=I," \
                                       "x:opt=BCTS," \
                                       "x:lab=," \
                                       "below:l=", \
                "-p", "freq", "-c", ":1:x:view=0.075:0.45," \
                                       "y:view=0.15:0.7," \
                                       "subint=I," \
                                       "pol=I," \
                                       "%s," \
                                       "cmap:map=plasma" % info, \
                "-p", "time", "-c", ":2:x:view=0.575:0.95," \
                                       "y:view=0.15:0.7," \
                                       "chan=I," \
                                       "pol=I," \
                                       "cmap:map=plasma"]
        utils.execute(cmd)


Diagnostic = CompositePlotDiagnostic
