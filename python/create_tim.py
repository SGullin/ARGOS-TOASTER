#!/usr/bin/python

from os import system, popen
from sys import argv, exit
from os import system, popen
from MySQLdb import *
import os.path
import argparse
import epta_pipeline_utils as epta

# define an exception that will be used if the arguments are bad:
class ArgError(Exception):
     """Exception called when there is a problem with the argument not caught by the regular ArgumentError found by argparse
    
     Attributes:
          name    -- argument name
          errtype -- which type of error occured
          output  -- explanation of the error
          """

     def __init__(self, prog, name, errtype):
          if (errtype == 0):
               self.output = prog + ": error: one or both arguments to "+ \
               name +" are negative."
          elif (errtype == 1):
               self.output = prog + ": First argument to option "+ name +\
               " is larger than the second."
          elif (errtype == 2):  # for toa_id
               self.output = prog + ": At least one argument to option "+ name +\
               " is greater than 1."
          else:
               self.output = prog + ": Unidentified error with argument "+\
               name +"."

def get_tim_opt(progname):
     #parse command line
     parser = argparse.ArgumentParser(
              prog=progname, 
              description='Extracts TOA information from table, and creates a tim file for use with tempo2.')

     parser.add_argument('--psr',
                         nargs=1, 
                         required=True, 
                         help='pulsar name, in J2000 convention')
     parser.add_argument('--obs', 
                         nargs='+',
                         help='observatory code(s)')
     parser.add_argument('--toa_id', 
                         nargs='+',
                         type=int,
                         help='individual TOA ID')
     parser.add_argument('--mjd', 
                          nargs=2,
                          type=float,
                          help='MJD range within which to retrieve TOAs')
     parser.add_argument('--mjderr', 
                          nargs=2,
                          type=float,
                          help='MJD error range within which to retrieve TOAs')
     parser.add_argument('--freq', 
                          nargs=2,
                          type=float,
                          help='frequency range within which to retreive TOAs')
     parser.add_argument('--outfile',
                         nargs=1, 
                       #  type=argparse.FileType('w'),
                         #default='output.tim', 
                         help='Name of output file')
     
     args=parser.parse_args()
    
     # Error check that MJDs and frequencies are positive and that the 
     # first is lower than the second argument, and that both are positive values
 
     # MJD
     if(args.mjd):
          try:
               argneg = (args.mjd[0] < 0 or args.mjd[1] < 0 )
               argdiffneg = (args.mjd[1] - args.mjd[0]) <= 0
               if(argneg):
                    raise ArgError(progname, '--mjd', 0)
               elif(argdiffneg):
                    raise ArgError(progname, '--mjd', 1)
          except ArgError, err:
               print err.output 

     # MJD error
     if(args.mjderr):
          try:
               argneg = (args.mjderr[0] < 0 or args.mjderr[1] < 0 )
               argdiffneg = (args.mjderr[1] - args.mjderr[0]) <= 0
               if(argneg):
                    raise ArgError(progname, '--mjderr', 0)
               elif(argdiffneg):
                    raise ArgError(progname, '--mjderr', 1)
          except ArgError, err:
               print err.output 

     # freq
     if(args.freq):
          try:
               argneg = (args.freq[0] < 0 or args.freq[1] < 0 )
               argdiffneg = (args.freq[1] - args.freq[0]) <= 0
               if(argneg):
                    raise ArgError(progname, '--freq', 0)
               elif(argdiffneg):
                    raise ArgError(progname, '--freq', 1)
          except ArgError, err:
               print err.output 

     # freq
     if(args.toa_id):
          try:
               for i_toa in range(len(args.toa_id)):
                    smallarg = (args.toa_id[i_toa] < 1 )
                    if(smallarg):
                         raise ArgError(progname, '--toa_id', 2)
          except ArgError, err:
               print err.output 

     return args

def main():
     progname = 'create_tim'
     #Make DB connection
     DBcursor, DBconn = epta.DBconnect(epta.DB_HOST, epta.DB_NAME, epta.DB_USER, epta.DB_PASS)

     # Get command line arguments
     args = get_tim_opt(progname) 


     # Open output tim file
     if args.outfile:
          outfile = args.outfile[0]
     else:
          outfile = args.psr[0]+".tim"

     try:
         f_tim = open(outfile,'w')
     except IOError, (errno, strerror):
         print "IOError (%s): %s"%(errno, strerror)

     print "\nFile", outfile, "open.\n"
     f_tim.write("FORMAT 1\n\n")
     
     # Run MySQL query to select TOAs with user-specified restrictions
     # Tables : P = pulsars, S = obssystems, T = toa, R = rawfiles
     columns = "P.pulsar_name"\
             + ", S.code"\
             + ", T.freq, T.imjd, T.fmjd, T.toa_unc_us"\
             + ", R.filename"

     #fromtables = "toa AS T LEFT JOIN obssystems AS S ON T.obssystem_id=T.obssystem_id"\
     #	        + " LEFT JOIN pulsars as P ON T.pulsar_id=P.pulsar_id" 
     fromtables = "toa AS T LEFT JOIN obssystems AS S ON T.obssystem_id=S.obssystem_id "\
	        + "LEFT JOIN pulsars as P ON T.pulsar_id=P.pulsar_id "\
	        + "LEFT JOIN rawfiles as R ON T.rawfile_id=R.rawfile_id" 

     # Now get constraints on query, one by one, based on command-line arguments:
     constraints = ["pulsar_name = '%s'"%args.psr[0]]
     
     # OBS
     if(args.obs):
          constraints.append('(' + ' OR '.join("S.code = '%s'"% args.obs[i_obs] for i_obs in range(len(args.obs))) + ')')
          # for i_obs in range(len(args.obs)-1):
            #   constraints.append('obs = '+args.obs[i_obs])

     # MJD
     if(args.mjd):
          constraints.append('T.imjd+T.fmjd >= '+repr(args.mjd[0])+' AND T.imjd+T.fmjd <= '+repr(args.mjd[1]))

     # MJD ERR
     if(args.mjderr):
          constraints.append('T.toa_unc_us >= '+repr(args.mjderr[0])+' AND T.toa_unc_us <= '+repr(args.mjderr[1]))

     # FREQ
     if(args.freq):
          constraints.append('T.freq >= '+repr(args.freq[0])+' AND T.freq <= '+repr(args.freq[1]))

     #TOA_ID
     if(args.toa_id):
          constraints.append('(' + ' OR '.join("toa_id = '%s'"% repr(args.toa_id[i_obs]) for i_obs in range(len(args.toa_id))) + ')')

     # print "columns = "+columns
     # print "constraints = ", ' AND '.join(constraints)
     constraints = ' AND '.join(constraints)

     # Now perform query on desired toas:
     #QUERY = "SELECT "+columns+" FROM toa WHERE "+constraints
     QUERY = "SELECT "+columns+" FROM "+fromtables+" WHERE "+constraints
     print "Constructing TOA tempo2-compatible file %s for pulsar PSR J%s, with the following query:\n\n%s\n"%(outfile, args.psr[0], QUERY)
     # Run the query 
     DBcursor.execute(QUERY)
     # Get query output
     DBOUT = DBcursor.fetchall()
     # Assign each DBOUT element to a separate array
     psr_name = []
     freq = []
     imjd = []
     fmjd = []
     toa_unc_us = []
     obs = []
     rawfilename = []
     for i_row in range(len(DBOUT)):
          psr_name.append(DBOUT[i_row][0])
          obs.append(str(DBOUT[i_row][1]))
          freq.append("%.2lf"%(DBOUT[i_row][2]))
          imjd.append("%5d"%(DBOUT[i_row][3]))
          fmjd.append("%.15lf"%(DBOUT[i_row][4]))
          toa_unc_us.append("%.4lf"%(DBOUT[i_row][5]))
          rawfilename.append(DBOUT[i_row][6])

    

     # print "psr_name\tobs\tfreq\timjd\tfmjd\t\ttoa_unc_us"
     #  print "psrfits_id\tfile_name\tnbin\tnchan\tnpol\tnsubint\tsite"
     #for i_row in range(len(DBOUT)):
          #print "\t".join("%s" % val for val in DBOUT[i_row])
 
     # Figure out how to add flags HERE.

     # Construct simple TOA lines in accordance with tempo2 format
     for i_row in range(len(DBOUT)):
          cur_line = [" %s "%rawfilename[i_row], \
                      freq[i_row],     \
                      "%s%s  "%(imjd[i_row], fmjd[i_row][1:]), \
                      toa_unc_us[i_row], \
                      obs[i_row]]
          #print cur_line
          cur_line_str = "  ".join(cur_line)+"\n"
          # print cur_line_str
          f_tim.write(cur_line_str)
     # print "DBOUT = ", DBOUT


     # Close profile file
     f_tim.close()

     print "File", outfile, "closed."

############ END OF MAIN #############

# Run create_tim
main()


