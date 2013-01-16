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

import sys

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


def get_pulsarinfo(existdb=None):
    """Return a dictionary of info for all pulsars.

        Inputs:
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
                            db.parfiles.c.dm, \
                            (1.0/db.parfiles.c.f0).label('period'), \
                            db.parfiles.c.raj, \
                            db.parfiles.c.decj, \
                            (db.parfiles.c.binary_model != None).\
                                label('is_binary')], \
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
        pulsarname_cache = utils.get_pulsarname_cache(existdb=db, \
                                                        update=True)
        pulsaralias_cache = utils.get_pulsaralias_cache(existdb=db, \
                                                                update=True)
        userinfo_cache = utils.get_userinfo_cache(existdb=db, update=True)
        telescopeinfo_cache = utils.get_telescopeinfo_cache(existdb=db, \
                                                                update=True)
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
        psrinfo[name] = {'id': psrid, \
                         'aliases': pulsaralias_cache[psrid], \
                         'telescopes': [], \
                         'curators': [], \
                         'numobs': 0, \
                         'numtoas': 0, \
                         'period': None, \
                         'dm': None, \
                         'raj': 'Unknown', \
                         'decj': 'Unknown', \
                         'is_binary': 'Unknown'}
    print rawfile_rows
    for row in rawfile_rows:
        psr = psrinfo[pulsarname_cache[row['pulsar_id']]]
        telname = telescopeinfo_cache[row['telescop'].lower()]['telescope_name']
        psr['telescopes'].append(telname)
        psr['numobs'] += row['numobs']
        print psr
    for row in numtoas_rows:
        psr = psrinfo[pulsarname_cache[row['pulsar_id']]]
        psr['numtoas'] += row['numtoas']
    for row in parfile_rows:
        psr = psrinfo[pulsarname_cache[row['pulsar_id']]]
        psr['period'] = row['period']
        psr['dm'] = row['dm']
        psr['raj'] = row['raj']
        psr['decj'] = row['decj']
        psr['is_binary'] = row['is_binary']
    for row in curator_rows:
        psr = psrinfo[pulsarname_cache[row['pulsar_id']]]
        if row['user_id'] is None:
            psr['curators'] = 'Everyone'
            break
        real_name = userinfo_cache[row['user_id']]['real_name'] 
        psr['curators'].append(real_name)
    return psrinfo


def show_pulsars(pulsar_ids):
    """Print pulsars and aliases to screen in a human-readable
        format.

        Input:
            pulsar_ids: list of pulsar IDs to display.

        Outputs:
            None
    """
    # Grab the pulsar alias cache once rather than accessing it multiple times
    pulsaralias_cache = utils.get_pulsaralias_cache()
    print "--"*25
    for id in sorted(pulsar_ids):
        psrname = utils.get_pulsarname(id)
        print colour.cstring("Pulsar ID:", underline=True, bold=True) + \
                colour.cstring(" %d" % id, bold=True)
        print "Pulsar Name: %s" % psrname 
        print "Aliases:"
        for alias in pulsaralias_cache[id]:
            if alias == psrname:
                continue
            print "    %s" % alias
        print "--"*25


def dump_pulsars(pulsar_ids):
    """Dump pulsar names and aliases to screen.

        Input:
            pulsar_ids: list of pulsar IDs to display.

        Outputs:
            None
    """
    # Grab the pulsar alias cache once rather than accessing it multiple times
    pulsaralias_cache = utils.get_pulsaralias_cache()
    for id in sorted(pulsar_ids):
        psrname = utils.get_pulsarname(id)
        print psrname
        for alias in pulsaralias_cache[id]:
            if alias == psrname:
                continue
            print alias



def main(args):
    # Get pulsar info
    psrinfo = get_pulsarinfo()
    print psrinfo
    sys.exit(1)
    if args.output_style=='text':
        show_pulsars(pulsar_ids)
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
