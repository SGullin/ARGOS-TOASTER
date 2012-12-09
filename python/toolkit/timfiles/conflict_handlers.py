"""
TOA conflict handlers to use when selecting TOAs for timfiles.

Patrick Lazarus, Dec 9, 2012
"""

import errors


def strict_conflict_handler(toas):
    """Check to see if there are any conflicts between TOAs.
        Raise an error if there is.

        Input:
            toas: The list of TOAs (ie rows returned from the DB)

        Outputs:
            toas: The input list of TOAs. (Only if no conflict is
                found).
    """
    # Collect information to see if there are any conflicts
    rawfile_ids = {}
    obssystem_ids = {}
    pulsar_ids = {}
    for toa in toas:
        if toa['replacement_rawfile_id'] is not None:
            raise errors.ConflictingToasError("Rawfile (ID: %d) has been " \
                    "replaced (by rawfile_id=%d)!" % \
                    (toa['rawfile_id'], toa['replacement_rawfile_id']))
        rawfile_ids.setdefault(toa['rawfile_id'], set()).\
                                add(toa['process_id'])
        obssystem_ids.setdefault(toa['obssystem_id'], set()).\
                                add(toa['template_id'])
        pulsar_ids.setdefault(toa['pulsar_id'], set()).\
                                add(toa['parfile_id'])
    # Respond to any conflicts
    if len(pulsar_ids) > 1:
        raise errors.ConflictingToasError("All TOAs must be for the same " \
                                "pulsar!")
    for procids in rawfile_ids.values():
        if len(procids) > 1:
            raise errors.ConflictingToasError("Some TOAs come from the same " \
                                "data file, but different processing jobs!")
    for tempids in obssystem_ids.values():
        if len(tempids) > 1:
            raise errors.ConflictingToasError("Some TOAs are from the same " \
                                "observing system, but have been generated " \
                                "with different templates!")
    for parids in pulsar_ids.values():
        if len(parids) > 1:
            raise errors.ConflictingToasError("Some TOAs are from the same " \
                                "pulsar, but have a different ephemeris " \
                                "installed!")
    return toas 


def tolerant_conflict_handler(toas):
    """Check to see if there are any TOAs from the same data file,
        but different processing runs.
        Raise an error if there is.

        Input:
            toas: The list of TOAs (ie rows returned from the DB)

        Outputs:
            toas: The input list of TOAs. (Only if no conflict is
                found).
    """
    # Collect information to see if there are any conflicts
    rawfile_ids = {}
    obssystem_ids = {}
    pulsar_ids = {}
    for toa in toas:
        if toa['replacement_rawfile_id'] is not None:
            warnings.warn("Rawfile (ID: %d) has been replaced (by " \
                    "rawfile_id=%d)!" % \
                    (toa['rawfile_id'], toa['replacement_rawfile_id']), \
                    errors.ToasterWarning)
        rawfile_ids.setdefault(toa['rawfile_id'], set()).\
                                add(toa['process_id'])
        obssystem_ids.setdefault(toa['obssystem_id'], set()).\
                                add(toa['template_id'])
        pulsar_ids.setdefault(toa['pulsar_id'], set()).\
                                add(toa['parfile_id'])
    # Respond to any conflicts
    if len(pulsar_ids) > 1:
        raise errors.ConflictingToasError("All TOAs must be for the same " \
                                "pulsar!")
    for procids in rawfile_ids.values():
        if len(procids) > 1:
            raise errors.ConflictingToasError("Some TOAs come from the same " \
                                "data file, but different processing jobs!")
    for tempids in obssystem_ids.values():
        if len(tempids) > 1:
            warnings.warn("Some TOAs are from the same observing " \
                          "system, but have been generated with " \
                          "different templates!", errors.ToasterWarning)
    for parids in pulsar_ids.values():
        if len(parids) > 1:
            warnings.warn("Some TOAs are from the same " \
                          "pulsar, but have a different ephemeris " \
                          "installed!", errors.ToasterWarning)
    return toas 


def get_newest_toas(toas):
    """Get TOAs. If there are conflicts take TOAs from the
        most recent processing job.

        Inputs:
            toas: The list of TOAs (ie rows returned from the DB)

        Outputs:
            toas: The (*in-place*) modified list of input TOAs, 
                using only those from the most recent processing 
                run for each data file.
    """
    # Sort TOAs by when they were produced
    toas.sort(key=lambda x: x['add_time'], reverse=True)
    rawfile_ids = {}
    obssystem_ids = {}
    pulsar_ids = {}
    ii = 0
    while ii < len(toas):
        toa = toas[ii]
        procids = rawfile_ids.setdefault(toa['rawfile_id'], set())
        if len(procids) and toa['process_id'] not in procids:
            toas.pop(ii)
            continue
        else:
            procids.add(toa['process_id'])
            ii += 1
        obssystem_ids.setdefault(toa['obssystem_id'], set()).\
                                add(toa['template_id'])
        pulsar_ids.setdefault(toa['pulsar_id'], set()).\
                                add(toa['parfile_id'])
    # Ensure all TOAs are from the same pulsar
    if len(pulsar_ids) > 1:
        raise errors.ConflictingToasError("All TOAs must be for the same " \
                                "pulsar!")
    # Warn if other minor conflicts were found
    for tempids in obssystem_ids.values():
        if len(tempids) > 1:
            warnings.warn("Some TOAs are from the same observing " \
                          "system, but have been generated with " \
                          "different templates!", errors.ToasterWarning)
    for parids in pulsar_ids.values():
        if len(parids) > 1:
            warnings.warn("Some TOAs are from the same " \
                          "pulsar, but have a different ephemeris " \
                          "installed!", errors.ToasterWarning)
    return toas



