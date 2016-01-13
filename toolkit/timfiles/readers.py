"""
TOA reader functions.

Patrick Lazarus, Apr. 2, 2013
"""
import re

from toaster.utils import notify
from toaster.utils import cache

# The following dictionary specifies the data type for
# expected flags. All keys should be lower case.
KNOWN_FLAG_TYPES = {'bw': float,
                    'length': float,
                    'nbin': int,
                    'padd': float,
                    'goodness_of_fit': float}

# The following dictionary specifies alternative names for
# expected flags. All keys should be lower case.
KNOWN_FLAG_ALIASES = {'bandwidth': 'bw',
                      'gof': 'goodness_of_fit'}


def tempo2_reader(line, get_telescope_id=True):
    """Parse line, assuming it is a TOA in tempo2 format.
        Return a dictionary of information.

        Input:
            line: A single TOA line in Tempo2 format.
            get_telescope_id: Query the database to get the telescope
                ID number. (Default: True)

        Output:
            toainfo: A dictionary of TOA information.
    """
    tempo2_toa_re = re.compile(r'^ *(?P<bad>(#|(C )|(c ))(?P<comment1>.*?))? *'
                               r'(?P<file>[^ ]+) +'
                               r'(?P<freq>\d+(\.\d+)?) +(?P<imjd>\d+)(?P<fmjd>\.\d+) +'
                               r'(?P<err>\d+(\.\d+)?) +(?P<site>[^ ]+)')
    comment_re = re.compile(r'#(?P<comment>.*)$')
    tempo2_flag_re = re.compile(r'-(?P<flagkey>[^ ]+) +(?P<flagval>[^ ]+)')
    match = tempo2_toa_re.search(line)
    if match is None:
        toainfo = None
        notify.print_debug("Line is not a Tempo2 TOA:\n    %s" % line, 'toaparse')
    else:
        grp = match.groupdict()
     
        toainfo = {}
        toainfo['grp'] = grp
        toainfo['is_bad'] = (line.strip().startswith('#') or line.strip().lower().startswith('c ')) #(grp['bad'] is not None)
        toainfo['file'] = grp['file']
        toainfo['freq'] = float(grp['freq'])
        toainfo['imjd'] = int(grp['imjd'])
        toainfo['fmjd'] = float(grp['fmjd'])
        toainfo['toa_unc_us'] = float(grp['err'])
        toainfo['telescope'] = grp['site']
        toainfo['line'] = line
        if get_telescope_id:
            toainfo['telescope_id'] = cache.get_telescope_info(grp['site'])['telescope_id']
        comments = []
        if grp['comment1']:
            comments.append(grp['comment1'].strip())
        match2 = comment_re.search(line[match.end():])
        if match2:
            grp2 = match2.groupdict()
            if grp2['comment']:
                comments.append(grp2['comment'].strip())
        toainfo['comment'] = " -- ".join(comments)
            
        toainfo['extras'] = {}
        for key, val in tempo2_flag_re.findall(line[match.end():]):
            key = key.lower()
            key = KNOWN_FLAG_ALIASES.get(key, key)
            caster = KNOWN_FLAG_TYPES.get(key, str)
            try:
                toainfo['extras'][key] = caster(val.strip())
            except:
                notify.print_info("Couldn't cast %s:%s" % (key, val), 2)

    notify.print_debug("TOA line: %s\nParsed info: %s" % (line, toainfo),
                    'toaparse')

    return toainfo


def parkes_reader(line, get_telescope_id=True):
    """Parse line, assuming it is a TOA in parkes format.
        Return a dictionary of information.

        Input:
            line: A single TOA line in parkes format.
            get_telescope_id: Query the database to get the telescope
                ID number. (Default: True)

        Output:
            toainfo: A dictionary of TOA information.
    """
    parkes_toa_re = re.compile(r'^ *?(?P<bad>(#|(C ))(?P<comment1>.*?))?'
                               r' (?P<info>.{24})(?P<freq>.{9})(?P<imjd>.{7})(?P<fmjd>\..{12}) '
                               r'(?P<phaseoffset>.{8}) (?P<err>.{7})(?P<info2>.{7}) '
                               r'(?P<site>.)(?P<dmcorr>[^#]*)')
    comment_re = re.compile(r'#(?P<comment>.*)$')
    
    match = parkes_toa_re.search(line.rstrip())
    if match is None:
        toainfo = None
        notify.print_debug("Line is not a Parkes-format TOA:\n    %s" % line, 'toaparse')
    else:
        grp = match.groupdict()
        toainfo = {}
        toainfo['is_bad'] = (grp['bad'] is not None)
        toainfo['freq'] = float(grp['freq'])
        toainfo['imjd'] = int(grp['imjd'])
        toainfo['fmjd'] = float(grp['fmjd'])
        toainfo['toa_unc_us'] = float(grp['err'])
        toainfo['telescope'] = grp['site']
        if get_telescope_id:
            toainfo['telescope_id'] = cache.get_telescope_info(grp['site'])['telescope_id']
        toainfo['extras'] = {'phaseoffset': float(grp['phaseoffset']),
                             'infostr': grp['info'].strip() + ' -- ' + grp['info2'].strip()}
        if grp['dmcorr']:
            toainfo['extras']['dmcorr'] = float(grp['dmcorr'])

        comments = []
        if grp['comment1']:
            comments.append(grp['comment1'].strip())
        match2 = comment_re.search(line[match.end():])
        if match2:
            grp2 = match2.groupdict()
            if grp2['comment']:
                comments.append(grp2['comment'].strip())
        toainfo['comment'] = " -- ".join(comments)
            
    return toainfo
