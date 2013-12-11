import warnings

from toaster import database
from toaster import errors
from toaster import utils
from toaster.utils import notify

def is_gitrepo(repodir):
    """Return True if the given dir is a git repository.

        Input:
            repodir: The location of the git repository.

        Output:
            is_git: True if directory is part of a git repository. False otherwise.
    """
    notify.print_info("Checking if directory '%s' contains a Git repo..." % repodir, 2)
    try:
        cmd = ["git", "rev-parse"]
        stdout, stderr = utils.execute(cmd, dir=repodir, \
                                    stderr=open(os.devnull))
    except errors.SystemCallError:
        # Exit code is non-zero
        return False
    else:
        # Success error code (i.e. dir is in a git repo)
        return True


def is_gitrepo_dirty(repodir):
    """Return True if the git repository has local changes.

        Inputs:
            repodir: The location of the git repository.

        Output:
            is_dirty: True if git repository has local changes. False otherwise.
    """
    notify.print_info("Checking if Git repo at '%s' is dirty..." % repodir, 2)
    try:
        cmd = ["git", "diff", "--quiet"]
        stdout, stderr = utils.execute(cmd, dir=repodir)
    except errors.SystemCallError:
        # Exit code is non-zero
        return True
    else:
        # Success error code (i.e. no differences)
        return False


def get_githash(repodir):
    """Get the pipeline's git hash.

        Inputs:
            repodir: The location of the git repository.

        Output:
            githash: The githash
    """
    if is_gitrepo_dirty(repodir):
        warnings.warn("Git repository has uncommitted changes!", \
                        errors.ToasterWarning)
    cmd = ["git", "rev-parse", "HEAD"]
    stdout, stderr = utils.execute(cmd, dir=repodir)
    githash = stdout.strip()
    return githash


def get_version_id(existdb=None):
    """Get the pipeline version number.
        If the version number isn't in the database, add it.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            version_id: The version ID for the current pipeline/psrchive
                combination.
    """
    # Check to make sure the repositories are clean
    check_repos()
    # Get git hashes
    pipeline_githash = get_githash(os.path.dirname(__file__))
    if is_gitrepo(config.cfg.psrchive_dir):
        psrchive_githash = get_githash(config.cfg.psrchive_dir)
    else:
        warnings.warn("PSRCHIVE directory (%s) is not a git repository! " \
                        "Falling back to 'psrchive --version' for version " \
                        "information." % config.cfg.psrchive_dir, \
                        errors.ToasterWarning)
        cmd = ["psrchive", "--version"]
        stdout, stderr = utils.execute(cmd)
        psrchive_githash = stdout.strip()
    
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    db.begin() # open a transaction

    # Check to see if this combination of versions is in the database
    select = db.select([db.versions.c.version_id]).\
                where((db.versions.c.pipeline_githash==pipeline_githash) & \
                      (db.versions.c.psrchive_githash==psrchive_githash) & \
                      (db.versions.c.tempo2_cvsrevno=='Not available'))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if len(rows) > 1:
        db.rollback()
        if not existdb:
            # Close the DB connection we opened
            db.close()
        raise errors.DatabaseError("There are too many (%d) matching " \
                                    "version IDs" % len(rows))
    elif len(rows) == 1:
        version_id = rows[0].version_id
    else:
        # Insert the current versions
        ins = db.versions.insert()
        values = {'pipeline_githash':pipeline_githash, \
                  'psrchive_githash':psrchive_githash, \
                  'tempo2_cvsrevno':'Not available'}
        result = db.execute(ins, values)
        # Get the newly add version ID
        version_id = result.inserted_primary_key[0]
        result.close()
    
    db.commit()
    
    if not existdb:
        # Close the DB connection we opened
        db.close()
    return version_id


def check_repos():
    """Check git repositories for the pipeline code, and for PSRCHIVE.
        If the repos are dirty raise and error.

        Inputs:
            None

        Outputs:
            None
    """
    if is_gitrepo_dirty(os.path.abspath(os.path.dirname(__file__))):
        if config.debug.GITTEST:
            warnings.warn("Git repository is dirty! Will tolerate because " \
                            "pipeline debugging is on.", \
                            errors.ToasterWarning)
        else:
            raise errors.ToasterError("Pipeline's git repository is dirty. " \
                                            "Aborting!")
    if not is_gitrepo(config.cfg.psrchive_dir):
        warnings.warn("PSRCHIVE directory (%s) is not a git repository!" % \
                        config.cfg.psrchive_dir, errors.ToasterWarning)
    elif is_gitrepo_dirty(config.cfg.psrchive_dir):
        raise errors.ToasterError("PSRCHIVE's git repository is dirty. " \
                                        "Clean up your act!")

