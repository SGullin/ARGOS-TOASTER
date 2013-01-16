#!/usr/bin/env python

"""
This TOASTER utility script provides the user with a listing
of pulsars in the database.

Patrick Lazarus, Oct 28, 2012.
"""
import errors
import colour
import utils
import database


SHORTNAME = 'show'
DESCRIPTION = "Get a listing of pulsars " \
              "from the DB to help the user."


def add_arguments(parser):
    parser.add_argument('-p', '--psr', dest='psrnames', \
                        type=str, default=[], action='append', \
                        help="The pulsar to grab info for.")
    parser.add_argument('--pulsar-id', dest='pulsar_ids', \
                        type=int, default=[], action='append', \
                        help="IDs of pulsars to grab info for.")
    parser.add_argument("--output-style", default='text', \
                        dest='output_style', type=str, \
                        help="The following options control how " \
                        "pulsars are displayed. Recognized " \
                        "modes: 'text' - List pulsars and aliases in a " \
                        "human-readable format; 'dump' - Dump all " \
                        "pulsar names and aliases to screen. " \
                        "(Default: text).")


def get_pulsarinfo(pulsar_ids=None, existdb=None):
    """Return a dictionary of info for all pulsars.

        Inputs:
            pulsar_ids: A list of pulsar IDs to get info for.
                (Default: Get info for all pulsars).
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Output:
            psrinfo: A dictionary of pulsar info dictionaries.
    """
    db = database.Database()
    db.connect()
    trans = db.begin()

    try:
        # Get number of observations
        select = db.select([db.rawfiles.c.pulsar_id, \
                            db.rawfiles.c.telescop, \
                            database.sa.func.count(db.rawfiles.c.rawfile_id).\
                                label('numobs')], \
                    from_obj=[db.rawfiles.\
                        outerjoin(db.replacement_rawfiles, \
                            onclause=db.replacement_rawfiles.c.obsolete_rawfile_id == \
                                    db.rawfiles.c.rawfile_id)]).\
                    where(db.replacement_rawfiles.c.replacement_rawfile_id == None).\
                    group_by(db.rawfiles.c.pulsar_id, \
                             db.rawfiles.c.telescop)
        result = db.execute(select)
        rawfile_rows = result.fetchall()
        result.close()
 
        # Get number of TOAs
        select = db.select([db.toas.c.pulsar_id, \
                            database.sa.func.count(db.toas.c.toa_id).\
                                label('numtoas')]).\
                    group_by(db.toas.c.pulsar_id)
        result = db.execute(select)
        numtoas_rows = result.fetchall()
        result.close()
 
        # Get parfile info
        select = db.select([db.parfiles.c.pulsar_id, \
                            db.parfiles.c.parfile_id, \
                            db.parfiles.c.dm, \
                            (1.0/db.parfiles.c.f0).label('period'), \
                            db.parfiles.c.raj, \
                            db.parfiles.c.decj, \
                            db.parfiles.c.binary_model], \
                    from_obj=[db.master_parfiles.\
                        outerjoin(db.parfiles, \
                            onclause=db.parfiles.c.parfile_id == \
                                    db.master_parfiles.c.parfile_id)])
        result = db.execute(select)
        parfile_rows = result.fetchall()
        result.close()
       
        # Get curators
        select = db.select([db.curators])
        result = db.execute(select)
        curator_rows = result.fetchall()
        result.close()

        # Get pulsar names
        pulsarname_cache = utils.get_pulsarname_cache(existdb=db)
        pulsaralias_cache = utils.get_pulsaralias_cache(existdb=db)
        userinfo_cache = utils.get_userinfo_cache(existdb=db)
        telescopeinfo_cache = utils.get_telescopeinfo_cache(existdb=db)
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
    finally:
        if not existdb:
            db.close()

    psrinfo = {}
    for psrid, name in pulsarname_cache.iteritems():
        if pulsar_ids is not None and psrid not in pulsar_ids:
            continue
        psrinfo[psrid] = {'name': name, \
                         'aliases': pulsaralias_cache[psrid], \
                         'telescopes': [], \
                         'curators': [], \
                         'numobs': 0, \
                         'numtoas': 0, \
                         'parfile_id': None, \
                         'period': None, \
                         'dm': None, \
                         'raj': 'Unknown', \
                         'decj': 'Unknown', \
                         'binary': None}
    for row in rawfile_rows:
        psrid = row['pulsar_id']
        if pulsar_ids is not None and psrid not in pulsar_ids:
            continue
        psr = psrinfo[psrid]
        telname = telescopeinfo_cache[row['telescop'].lower()]['telescope_name']
        psr['telescopes'].append(telname)
        psr['numobs'] += row['numobs']
    for row in numtoas_rows:
        psrid = row['pulsar_id']
        if pulsar_ids is not None and psrid not in pulsar_ids:
            continue
        psr = psrinfo[psrid]
        psr['numtoas'] += row['numtoas']
    for row in parfile_rows:
        psrid = row['pulsar_id']
        if pulsar_ids is not None and psrid not in pulsar_ids:
            continue
        psr = psrinfo[psrid]
        psr['parfile_id'] = row['parfile_id']
        psr['period'] = row['period']
        psr['dm'] = row['dm']
        psr['raj'] = row['raj']
        psr['decj'] = row['decj']
        psr['binary'] = row['binary']
    for row in curator_rows:
        psrid = row['pulsar_id']
        if pulsar_ids is not None and psrid not in pulsar_ids:
            continue
        psr = psrinfo[psrid]
        if row['user_id'] is None:
            psr['curators'] = 'Everyone'
            break
        real_name = userinfo_cache[row['user_id']]['real_name'] 
        psr['curators'].append(real_name)
    return psrinfo


def show_pulsars(psrinfo):
    """Print pulsar info to screen in a human-readable format.

        Input:
            psrinfo: A dictionary of pulsar info dictionaries.
                (As returned by get_pulsarinfo(...))

        Outputs:
            None
    """
    print "--"*25
    for psrid in sorted(psrinfo.keys()):
        psr = psrinfo[psrid]
        print colour.cstring("Pulsar ID:", underline=True, bold=True) + \
                colour.cstring(" %d" % psrid, bold=True)
        print "Pulsar Name: %s" % psr['name'] 
        print "Aliases:"
        for alias in psr['aliases']:
            if alias == psr['name']:
                continue
            print "    %s" % alias
        if psr['parfile_id'] is None:
            print "No parfile loaded!"
        else:
            if psr['period'] > 1:
                print "Period: %.3f s" % psr['period']
            else:
                print "Period: %.2f ms" % (1000.0*psr['period'])
            print "DM: %.2f pc/cc" % psr['dm']
            print "R.A. (J2000): %s" % psr['raj']
            print "Dec. (J2000): %s" % psr['decj']
            print "Binary model: %s" % psr['binary']

        lines = ["Number of observations: %d" % psr['numobs']]
        if psr['numobs'] > 0:
            lines.append("Telescopes used:\n    " + \
                            "\n    ".join(psr['telescopes']))
        lines.append("Number of TOAs: %d" % psr['numtoas'])
        if psr['curators'] == 'Everyone':
            lines.append("Curators: Everyone")
        elif psr['curators']:
            lines.append("Curators:\n    " + \
                            "\n    ".join(psr['curators']))
        else:
            lines.append("Curators: None")
        utils.print_info("\n".join(lines), 1)
        print "--"*25


def dump_pulsars(pulsar_ids=None):
    """Dump pulsar names and aliases to screen.

        Input:
            pulsar_ids: list of pulsar IDs to display.
                (Default: dump all pulsars)

        Outputs:
            None
    """
    # Grab the pulsar alias cache once rather than accessing it multiple times
    pulsaralias_cache = utils.get_pulsaralias_cache()
    if pulsar_ids is None:
        pulsar_ids = sorted(pulsaralias_cache.keys())
    for psrid in sorted(pulsar_ids):
        psrname = utils.get_pulsarname(psrid)
        print psrname
        for alias in pulsaralias_cache[psrid]:
            if alias == psrname:
                continue
            print alias


def main(args):
    # Build caches
    utils.get_pulsarname_cache()
    pulsar_ids = args.pulsar_ids + \
                    [utils.get_pulsarid(psr) for psr in args.psrnames]
    if not pulsar_ids:
        pulsar_ids = None
    if args.output_style=='text':
        # Get pulsar info
        psrinfo = get_pulsarinfo(pulsar_ids)
        show_pulsars(psrinfo)
    elif args.output_style=='dump':
        dump_pulsars(pulsar_ids)
    else:
        raise errors.UnrecognizedValueError("The output-style '%s' is " \
                    "not recognized!" % args.output_style)


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
