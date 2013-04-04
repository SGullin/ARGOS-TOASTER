"""
TOA reader functions.

Patrick Lazarus, Apr. 2, 2013
"""
import re

import utils
import errors

# The following dictionary specifies the data type for
# expected flags. All keys should be lower case.
KNOWN_FLAG_TYPES = {'bw': float, \
                    'length': float, \
                    'nbin': int, \
                    'goodness_of_fit': float}

# The following dictionary specifies alternative names for
# expected flags. All keys should be lower case.
KNOWN_FLAG_ALIASES = {'bandwidth': 'bw', \
                      'gof': 'goodness_of_fit'}

def tempo2_reader(line):
    """Parse line, assuming it is a TOA in tempo2 format.
        Return a dictionary of information.

        Input:
            line: A single TOA line in Tempo2 format.

        Output:
            toainfo: A dictionary of TOA information.
    """
    tempo2_toa_re = re.compile(r'^ *(?P<bad>(#|(C ))(?P<comment1>.*?))? *' \
                               r'(?P<file>[^ ]+) +' \
                               r'(?P<freq>\d+(\.\d+)?) +(?P<imjd>\d+)(?P<fmjd>\.\d+) +' \
                               r'(?P<err>\d+(\.\d+)?) +(?P<site>[^ ]+)')
    comment_re = re.compile(r'#(?P<comment>.*)$')
    tempo2_flag_re = re.compile(r'-(?P<flagkey>[^ ]+) +(?P<flagval>[^ ]+)')
    match = tempo2_toa_re.search(line)
    if match is None:
        toainfo = None
        utils.print_debug("Line is not a Tempo2 TOA:\n    %s" % line, 'toaparse')
    else:
        grp = match.groupdict()
     
        toainfo = {}
        toainfo['is_bad'] = (grp['bad'] is not None)
        toainfo['file'] = grp['file']
        toainfo['freq'] = float(grp['freq'])
        toainfo['imjd'] = int(grp['imjd'])
        toainfo['fmjd'] = float(grp['fmjd'])
        toainfo['toa_unc_us'] = float(grp['err'])
        toainfo['telescope'] = grp['site']
        toainfo['telescope_id'] = utils.get_telescope_info(grp['site'])['telescope_id']
        comments = []
        if grp['comment1']:
            comments.append(grp['comment1'].strip())
        match2 = comment_re.search(line[match.end():])
        if match2:
            grp2 = match2.groupdict()
            if grp2['comment']:
                comments.append(grp2['comment'].strip())
        toainfo['comment'] = " -- ".join(comments)
            
        toainfo['flags'] = {}
        for key, val in tempo2_flag_re.findall(line[match.end():]):
            key = key.lower()
            key = KNOWN_FLAG_ALIASES.get(key, key)
            caster = KNOWN_FLAG_TYPES.get(key, str)
            toainfo['flags'][key] = caster(val)

    return toainfo
