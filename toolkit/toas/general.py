from toaster import config


def parse_pat_output(patout):
    """Parse the output from 'pat'.

        Input:
            patout: The stdout output of running 'pat'.

        Output:
            toainfo: A list of dictionaries, each with
                information for a TOA.
    """
    toainfo = []
    for toastr in patout.split("\n"):
        toastr = toastr.strip()
        if toastr and (toastr != "FORMAT 1") and \
                (not toastr.startswith("Plotting")):
            toasplit = toastr.split()
            freq = float(toasplit[1])
            imjd = float(toasplit[2].split(".")[0])
            fmjd = float("0." + toasplit[2].split(".")[1])
            err = float(toasplit[3])
            if '-gof' in toasplit:
                # The goodness-of-fit is only calculated for the 'FDM'
                # fitting method. The GoF value returned for other
                # methods is inaccurate.
                gofvalstr = toasplit[toasplit.index('-gof') + 1]
                if config.cfg.toa_fitting_method == 'FDM' and gofvalstr != '*error*':
                    gof = float(gofvalstr)
                else:
                    gof = None
            if ('-bw' in toasplit) and ('-nchan' in toasplit):
                nchan = int(toasplit[toasplit.index('-nchan') + 1])
                bw = float(toasplit[toasplit.index('-bw') + 1])
                bw_per_toa = bw / nchan
            else:
                bw_per_toa = None
            if ('-length' in toasplit) and ('-nsubint' in toasplit):
                nsubint = int(toasplit[toasplit.index('-nsubint') + 1])
                length = float(toasplit[toasplit.index('-length') + 1])
                length_per_toa = length / nsubint
            else:
                length_per_toa = None
            if '-nbin' in toasplit:
                nbin = int(toasplit[toasplit.index('-nbin') + 1])
            toainfo.append({'freq': freq,
                            'imjd': imjd,
                            'fmjd': fmjd,
                            'toa_unc_us': err,
                            'goodness_of_fit': gof,
                            'bw': bw_per_toa,
                            'length': length_per_toa,
                            'nbin': nbin})
    return toainfo