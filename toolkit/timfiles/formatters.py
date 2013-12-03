"""
TOA formatter functions.

Patrick Lazarus, Dec 9, 2012
"""
import config

def princeton_formatter(toas, flags=[]):
    """Return timfile lines in princeton format.
        
        Inputs:
            toas: A list of TOAs.
            flags: A single string containing flags to add to each TOA.
                NOTE: These are ignored! The princeton TOA format
                does _not_ support flags.

        Output:
            timlines: A list of lines to be written into the timfile.
    """
    timlines = []
    for toa in toas:
        fmjdstr = "%.13f" % toa['fmjd']
        mjd = ("%5d" % toa['imjd']) + (fmjdstr[fmjdstr.index('.'):])
        timlines.append("%s               %8.3f %s %8.2f" % \
                            (toa['telescope_code'], toa['freq'], \
                                mjd, toa['toa_unc_us']))
    return timlines
        

def tempo2_formatter(toas, flags=[]):
    """Return timfile lines in TEMPO2 format.
        
        Inputs:
            toas: A list of TOAs.
            flags: A single string of flags to add to each TOA.

        Output:
            timlines: A list of lines to be written into the timfile.
    """
    timlines = ["FORMAT 1"]
    for toa in toas:
        fmjdstr = str(toa['fmjd'])
        mjd = "%5d%s" % (toa['imjd'], fmjdstr[fmjdstr.index('.'):])
        toastr = "%s %.3f %s %.3f %s" % \
                    (toa['rawfile'], toa['freq'], mjd, \
                        toa['toa_unc_us'], toa['telescope_code'])
        flagstrs = []
        for name, valuetag in flags:
            try:
                value = valuetag % toa
            except TypeError:
                value = config.cfg.missing_flag_value
            if value is not None:
                flagstrs.append("-%s %s" % (name, value))
        timlines.append("%s %s" % (toastr, " ".join(flagstrs)))
    return timlines


