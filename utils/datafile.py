import hashlib
import warnings
import os.path
import shutil

from toaster import config
from toaster import errors
from toaster import utils
from toaster.utils import notify
from toaster.utils import cache


header_param_types = {'freq': float,
                      'length': float,
                      'bw': float,
                      'mjd': float,
                      'intmjd': int,
                      'fracmjd': float,
                      'backend': str,
                      'rcvr': str,
                      'telescop': str,
                      'name': str,
                      'nchan': int,
                      'npol': int,
                      'nbin': int,
                      'nsub': int,
                      'tbin': float}


def verify_file_path(fn):
    #Verify that file exists
    notify.print_info("Verifying file: %s" % fn, 2)
    if not os.path.isfile(fn):
        raise errors.FileError("File %s does not exist, you dumb dummy!" % fn)

    #Determine path (will retrieve absolute path)
    file_path, file_name = os.path.split(os.path.abspath(fn))
    notify.print_info("File %s exists!" % os.path.join(file_path, file_name), 3)
    return file_path, file_name


def get_header_vals(fn, hdritems):
    """Get a set of header params from the given file.
        Returns a dictionary.

        Inputs:
            fn: The name of the file to get params for.
            hdritems: List of parameters (recognized by vap) to fetch.

        Output:
            params: A dictionary. The keys are values requested from 'vap'
                the values are the values reported by 'vap'.
    """
    if not len(hdritems):
        raise ValueError("No 'hdritems' requested to get from file header!")
    hdrstr = ",".join(hdritems)
    if '=' in hdrstr:
        raise ValueError("'hdritems' passed to 'get_header_vals' "
                         "should not perform and assignments!")
    cmd = ["vap", "-n", "-c", hdrstr, fn]
    outstr, errstr = utils.execute(cmd)
    outvals = outstr.split()[(0 - len(hdritems)):]  # First value is filename (we don't need it)
    if errstr:
        raise errors.SystemCallError("The command: %s\nprinted to stderr:\n%s" %
                                     (cmd, errstr))
    elif len(outvals) != len(hdritems):
        raise errors.SystemCallError("The command: %s\nreturned the wrong "
                                     "number of values. (Was expecting %d, got %d.)" %
                                     (cmd, len(hdritems), len(outvals)))
    params = HeaderParams(fn)
    for key, val in zip(hdritems, outvals):
        if val == "INVALID":
            raise errors.SystemCallError("The vap header key '%s' "
                                         "is invalid!" % key)
        elif val == "*" or val == "UNDEF":
            warnings.warn("The vap header key '%s' is not "
                          "defined in this file (%s)" % (key, fn),
                          errors.ToasterWarning)
            params[key] = None
        else:
            # Get param's type to cast value
            caster = header_param_types.get(key, str)
            params[key] = caster(val)
    return params


def parse_psrfits_header(fn, hdritems):
    """Get a set of header params from the given file.
        Returns a dictionary.

        Inputs:
            fn: The name of the file to get params for.
            hdritems: List of parameter names to fetch.

        Output:
            params: A dictionary. The keys are values requested from 'psredit'
                the values are the values reported by 'psredit'.
    """
    hdrstr = ",".join(hdritems)
    if '=' in hdrstr:
        raise ValueError("'hdritems' passed to 'parse_psrfits_header' "
                         "should not perform and assignments!")
    cmd = ["psredit", "-q", "-Q", "-c", hdrstr, fn]
    outstr, errstr = utils.execute(cmd)
    outvals = outstr.split()
    if errstr:
        raise errors.SystemCallError("The command: %s\nprinted to stderr:\n%s" %
                                     (cmd, errstr))
    elif len(outvals) != len(hdritems):
        raise errors.SystemCallError("The command: %s\nreturn the wrong "
                                     "number of values. (Was expecting %d, got %d.)" %
                                     (cmd, len(hdritems), len(outvals)))
    params = {}
    for key, val in zip(hdritems, outstr.split()):
        params[key] = val
    return params


def prep_file(fn):
    """Prepare file for archiving/loading.
        
        Also, perform some checks on the file to make sure we
        won't run into problems later. Checks peformed:
            - Existence of file.
            - Read/write access for file (so it can be moved).
            - Header contains all necessary values.
            - Site/observing system is recognized.

        Input:
            fn: The name of the file to check.

        Outputs:
            params: A dictionary of info to be uploaded.
    """
    # Check existence of file
    verify_file_path(fn)

    # Check file permissions allow for writing and reading
    if not os.access(fn, os.R_OK):
        raise errors.FileError("File (%s) is not readable!" % fn)

    # Grab header info
    hdritems = ["nbin", "nchan", "npol", "nsub", "type", "telescop",
                "name", "dec", "ra", "freq", "bw", "dm", "rm",
                # The names of these header params
                # vary with psrchive version
                # "dmc", "rm_c", "pol_c",
                "scale", "state", "length",
                "rcvr", "basis", "backend", "mjd"]

    params = get_header_vals(fn, hdritems)

    # Normalise telescope name
    tinfo = cache.get_telescope_info(params['telescop'])
    params['telescop'] = tinfo['telescope_name']
    params.update(tinfo)

    # Check if obssystem_id, pulsar_id, user_id can be found
    obssys_key = (params['telescop'].lower(), params['rcvr'].lower(),
                  params['backend'].lower())
    obssys_ids = cache.get_obssystemid_cache()
    if obssys_key not in obssys_ids:
        t, r, b = obssys_key
        raise errors.FileError("The observing system combination in the file "
                               "%s is not registered in the database. "
                               "(Telescope: %s, Receiver: %s; Backend: %s)." %
                               (fn, t, r, b))
    else:
        params['obssystem_id'] = obssys_ids[obssys_key]
        obssysinfo = cache.get_obssysinfo(params['obssystem_id'])
        params['band_descriptor'] = obssysinfo['band_descriptor']
        params['obssys_name'] = obssysinfo['name']

    # Check if pulsar_id is found
    try:
        psr_id = cache.get_pulsarid(params['name'])
    except errors.UnrecognizedValueError:
        if config.cfg.auto_add_pulsars:
            notify.print_info("Automatically inserting pulsar with "
                              "name '%s'" % params['name'], 1)
            # Force an update of the pulsarid cache
            cache.get_pulsarid_cache(update=True)
        else:
            raise errors.FileError("The pulsar name %s (from file %s) is not "
                                   "recognized." % (params['name'], fn))
    else:
        # Normalise pulsar name
        params['name'] = cache.get_prefname(params['name'])
        params['pulsar_id'] = psr_id

        params['user_id'] = cache.get_userid()
    return params


def archive_file(toarchive, destdir):
    if not config.cfg.archive:
        # Configured to not archive files
        warnings.warn("Configurations are set to _not_ archive files. "
                      "Doing nothing...", errors.ToasterWarning)
        return toarchive
    srcdir, fn = os.path.split(toarchive)
    dest = os.path.join(destdir, fn)

    # Check if the directory exists
    # If not, create it
    if not os.path.isdir(destdir):
        # Set permissions (in octal) to read/write/execute for user and group
        notify.print_info("Making directory: %s" % destdir, 2)
        os.makedirs(destdir, 0770)

    # Check that our file doesn't already exist in 'dest'
    # If it does exist do nothing but print a warning
    if not os.path.isfile(dest):
        # Copy file to 'dest'
        notify.print_info("Moving %s to %s" % (toarchive, dest), 2)
        shutil.copy2(toarchive, dest)

        # Check that file copied successfully
        srcmd5 = get_md5sum(toarchive)
        srcsize = os.path.getsize(toarchive)
        destmd5 = get_md5sum(dest)
        destsize = os.path.getsize(dest)
        if (srcmd5 == destmd5) and (srcsize == destsize):
            if config.cfg.move_on_archive:
                os.remove(toarchive)
                notify.print_info("File (%s) successfully moved to %s." %
                                  (toarchive, dest), 2)
            else:
                notify.print_info("File (%s) successfully copied to %s." %
                                  (toarchive, dest), 2)
        else:
            raise errors.ArchivingError("File copy failed! (Source MD5: %s, "
                                        "Dest MD5: %s; Source size: %d, Dest size: %d)" %
                                        (srcmd5, destmd5, srcsize, destmd5))
    elif os.path.abspath(destdir) == os.path.abspath(srcdir):
        # File is already located in its destination
        # Do nothing
        warnings.warn("Source file %s is already in the archive (and in "
                      "the correct place). Doing nothing..." % toarchive,
                      errors.ToasterWarning)
        pass
    else:
        # Another file with the same name is the destination directory
        # Compare the files
        srcmd5 = get_md5sum(toarchive)
        srcsize = os.path.getsize(toarchive)
        destmd5 = get_md5sum(dest)
        destsize = os.path.getsize(dest)
        if (srcmd5 == destmd5) and (srcsize == destsize):
            # Files are the same, so remove src as if we moved it
            # (taking credit for work that was already done...)
            warnings.warn("Another version of this file (%s), with "
                          "the same size (%d bytes) and the same "
                          "MD5 (%s) is already in the archive. "
                          "Doing nothing..." %
                          (toarchive, destsize, destmd5),
                          errors.ToasterWarning)
        else:
            # The files are not the same! This is not good.
            # Raise an exception.
            raise errors.ArchivingError("File (%s) cannot be archived. "
                                        "There is already a file archived by that name "
                                        "in the appropriate archive location (%s), but "
                                        "the two files are _not_ identical. "
                                        "(source: MD5=%s, size=%d bytes; dest: MD5=%s, "
                                        "size=%d bytes)" %
                                        (toarchive, dest, srcmd5, srcsize, destmd5, destsize))

    # Change permissions so the file can no longer be written to
    notify.print_info("Changing permissions of archived file to 440", 2)
    os.chmod(dest, 0440)  # "0440" is an integer in base 8. It works
    # the same way 440 does for chmod on cmdline

    notify.print_info("%s archived to %s (%s)" % (toarchive, dest, utils.give_utc_now()), 1)

    return dest


def get_archive_dir(fn, data_archive_location=None, params=None):
    """Given a file name return where it should be archived.

        Input:
            fn: The name of the file to archive.
            data_archive_location: The base directory of the 
                archive. (Default: use location listed in config
                file).
            params: A HeaderParams object containing header
                parameters of the data file. (Default: create
                a throw-away HeaderParams object and populate
                it as necessary). NOTE: A dictionary object
                containing the required params can also be used.

        Output:
            dir: The directory where the file should be archived.
    """
    if data_archive_location is None:
        data_archive_location = config.cfg.data_archive_location
    if params is None:
        params = get_header_vals(fn, [])

    subdir = config.cfg.data_archive_layout % params
    archivedir = os.path.join(data_archive_location, subdir)
    archivedir = os.path.abspath(archivedir)
    if not archivedir.startswith(os.path.abspath(data_archive_location)):
        raise errors.ArchivingError("Archive directory for '%s' (%s) is "
                                    "not inside the specified data archive location: %s. "
                                    "Please check the 'data_archive_layout' parameter in "
                                    "the config file." %
                                    (fn, archivedir, data_archive_location))
    return archivedir


null = lambda x: x


class FancyParams(dict):
    def __init__(self, *args, **kwargs):
        super(FancyParams, self).__init__(*args, **kwargs)

    def __getitem__(self, key):
        if (type(key) in (type('str'), type(u'str'))) and key.endswith("_L"):
            filterfunc = str.lower
            key = key[:-2]
        elif (type(key) in (type('str'), type(u'str'))) and key.endswith("_U"):
            filterfunc = str.upper
            key = key[:-2]
        else:
            filterfunc = null
        if self.has_key(key):
            val = self.get_value(key)
            if type(val) in (type('str'), type(u'str')):
                val = str(val)
                return filterfunc(val)
            else:
                return val
        else:
            matches = [k for k in self.keys() if k.startswith(key)]
            if len(matches) == 1:
                val = self.get_value(matches[0])
                if type(val) in (type('str'), type(u'str')):
                    return filterfunc(val)
                else:
                    return val
            elif len(matches) > 1:
                raise errors.UnrecognizedValueError("The header parameter "
                                                    "abbreviation '%s' is ambiguous. ('%s' "
                                                    "all match)" %
                                                    (key, "', '".join(matches)))
            else:
                val = self.get_value(key)
                if type(val) in (type('str'), type(u'str')):
                    return filterfunc(val)
                else:
                    return val

    def get_value(self, key):
        if key not in self:
            params = self._generate_value(key)
            self.update(params)
        try:
            val = super(FancyParams, self).__getitem__(key)
        except KeyError:
            raise errors.BadColumnNameError("Unrecognized parameter "
                                            "name (%s). Recognized parameters: '%s'" %
                                            (key, "', '".join(sorted(self.keys()))))
        return val

    def _generate_value(self, key):
        """To generate a missing value, if it is not contained in
            the Params object.

            NOTE: This method must be implemented by child classes.
    
            Input:
                key: The key of the value to generate.

            Outputs:
                param: The generated (formerly missing) value.
        """
        raise NotImplementedError("Cannot generate value (key: %s). "
                                  "'%s' class of should provide an implementation "
                                  "of '_generate_value" % (key, self.__class__.__name__))


class HeaderParams(FancyParams):
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        super(HeaderParams, self).__init__(*args, **kwargs)

    def _generate_value(self, key):
        return get_header_vals(self.fn, [key])


def get_md5sum(fname, block_size=16 * 8192):
    """Compute and return the MD5 sum for the given file.
        The file is read in blocks of 'block_size' bytes.

        Inputs:
            fname: The name of the file to get the md5 for.
            block_size: The number of bytes to read at a time.
                (Default: 16*8192)

        Output:
            md5: The hexidecimal string of the MD5 checksum.
    """
    ff = open(fname, 'rb')
    md5 = hashlib.md5()
    block = ff.read(block_size)
    while block:
        md5.update(block)
        block = ff.read(block_size)
    ff.close()
    return md5.hexdigest()