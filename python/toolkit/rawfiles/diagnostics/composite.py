import tempfile
import os
import shutil

import utils
import base

class CompositePlotDiagnostic(base.PlotDiagnostic):
    name = 'Composite'

    def _compute(self):
        utils.print_info("Creating composite summary plot for %s" % self.fn, 3)
        handle, tmpfn = tempfile.mkstemp(suffix=".png")
        os.close(handle)
        params = utils.prep_file(self.fn)
        
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
                       "N\\dbin\\u=$nbin    N\\dchan\\u=$nchan    N\\dsub\\u=$nsubint\n" \
                       "Rawfile ID: %s," \
               "above:off=3.5" % \
                        (os.path.split(self.fn)[-1], \
                         params['telescop'], params['rcvr'], \
                         params['backend'], params['length'], params['bw'], \
                         self.rawfile_id)
        return info

    def __plot_profonly(self, tmpfn, params):
        if params['npol'] > 1:
            profplot = 'stokes'
            pol = ''
        else:
            profplot = 'flux'
            pol = 'pol=I,'
        info = self.__get_info(params)
        utils.execute("psrplot -O -j 'D' -c 'above:c=,x:range=0:2' %s -D %s/PNG " \
                        "-p %s -c ':0:x:view=0.075:0.95," \
                                       "y:view=0.15:0.7," \
                                       "subint=I," \
                                       "chan=I," \
                                       "%s" \
                                       "below:l=," \
                                       "%s'" % \
                            (self.fn, tmpfn, profplot, pol, info))
        
    def __plot_nofreq(self, tmpfn, params):
        profplot = 'flux'
        if params['npol'] > 1:
            profplot = 'stokes'
            pol = ''
        else:
            profplot = 'flux'
            pol = 'pol=I,'
        info = self.__get_info(params)
        utils.execute("psrplot -O -j 'D' -c 'above:c=,x:range=0:2' %s -D %s/PNG " \
                        "-p %s -c ':0:x:view=0.075:0.95," \
                                       "y:view=0.5:0.7," \
                                       "subint=I," \
                                       "chan=I," \
                                       "%s" \
                                       "x:opt=BCTS," \
                                       "x:lab=," \
                                       "below:l=," \
                                       "%s' " \
                        "-p time -c ':1:x:view=0.075:0.95," \
                                       "y:view=0.15:0.5," \
                                       "chan=I," \
                                       "pol=I," \
                                       "cmap:map=plasma'" % \
                            (self.fn, tmpfn, profplot, pol, info))
        
    def __plot_notime(self, tmpfn, params):
        profplot = 'flux'
        if params['npol'] > 1:
            profplot = 'stokes'
            pol = ''
        else:
            profplot = 'flux'
            pol = 'pol=I,'
        info = self.__get_info(params)
        utils.execute("psrplot -O -j 'D' -c 'above:c=,x:range=0:2' %s -D %s/PNG " \
                        "-p %s -c ':0:x:view=0.075:0.95," \
                                       "y:view=0.5:0.7," \
                                       "subint=I," \
                                       "chan=I," \
                                       "%s" \
                                       "x:opt=BCTS," \
                                       "x:lab=," \
                                       "below:l=," \
                                       "%s' " \
                        "-p freq -c ':1:x:view=0.075:0.95," \
                                       "y:view=0.15:0.5," \
                                       "subint=I," \
                                       "pol=I," \
                                       "cmap:map=plasma'" % \
                            (self.fn, tmpfn, profplot, pol, info))
        
    def __plot_all(self, tmpfn, params):
        if params['npol'] > 1:
            profplot = 'stokes'
            pol = ''
        else:
            profplot = 'flux'
            pol = 'pol=I,'
        info = self.__get_info(params)
        utils.execute("psrplot -O -j 'D' -c 'above:c=,x:range=0:2' %s -D %s/PNG " \
                        "-p %s -c ':0:x:view=0.575:0.95," \
                                       "y:view=0.7:0.9," \
                                       "subint=I," \
                                       "chan=I," \
                                       "%s" \
                                       "x:opt=BCTS," \
                                       "x:lab=," \
                                       "below:l=' " \
                        "-p freq -c ':1:x:view=0.075:0.45," \
                                       "y:view=0.15:0.7," \
                                       "subint=I," \
                                       "pol=I," \
                                       "%s," \
                                       "cmap:map=plasma' " \
                        "-p time -c ':2:x:view=0.575:0.95," \
                                       "y:view=0.15:0.7," \
                                       "chan=I," \
                                       "pol=I," \
                                       "cmap:map=plasma'" % \
                            (self.fn, tmpfn, profplot, pol, info))


Diagnostic = CompositePlotDiagnostic
