#!/usr/bin/env python

import sqlite3
import sys
import getopt


# Globals
_separator = "\t"
_verbose = False
_conn = None
_cursor = None
_file = None

# Only print if in verbose mode
def logprint(msg):
	if(_verbose):
		print msg

# Global exit handler, make sure to close files
def exitprogram(status):
	global _conn
	global _cursor
	global _file

	if(_cursor != None):
		_cursor.close()
		
	if(_conn != None):
		_conn.commit()
		_conn.close()
		
	if(_file != None):
		_file.close()
	
	sys.exit(status)

def work(filename):
	global _separator
	global _conn
	global _cursor
	global _file

	fields = []

	tablename = filename.split('.')[0]
	logprint("INFO: Starting work, filename: %s, tablename: %s, separator: %s" % (filename, tablename, _separator))	

	try:
		_file = open(filename)
	except:
		print "ERROR: Could not open file '%s'" % filename
		exitprogram(1)

	_conn = sqlite3.connect(tablename + ".sqlite")
	_conn.text_factory = str
	_cursor = _conn.cursor()


	for i, line in enumerate(_file):
		row = line.strip().split(_separator)
		if(i == 0):
			logprint("INFO: Creating table")
			fields = row
			_cursor.execute('DROP TABLE IF EXISTS %s' % (tablename))
			_cursor.execute('CREATE TABLE %s (%s)' % (tablename, ",".join(fields)) )
			continue

		if len(row) != 0:
			if( len(row) != len(fields)):
				print "ERROR: Wrong number of fields on row %d" % (i)
				continue
			sql = 'INSERT INTO %s VALUES ("%s")' % (tablename, '","'.join(row))
			logprint("INFO: %s" % sql)
			_cursor.execute(sql)

	logprint("INFO: Fields '%s' inserted into '%s'" % ("','".join(fields), tablename))
	exitprogram(0)

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hs:v", ["help", "separator=", "verbose"])
	except getopt.GetoptError, err:
		usage(str(err))
		exitprogram(1)

	for o, a in opts:
		if o in ("-v", "--verbose"):
			global _verbose
			_verbose = True
		elif o in ("-h", "--help"):
			usage("Help")
			exitprogram(0)
		elif o in ("-s", "--separator"):
			global _separator
			if(len(a) != 0):
				_separator = a.decode('string-escape')
			else:
				usage("Invalid separator")
				exitprogram(1)
		else:
			assert False, "unhandled option"
			
	if(len(args) == 1):
		work(args[0])
	elif(len(args) == 0):
		usage("No input file")	
	else:
		usage("Only one input file allowed")


def usage(msg=""):
	prog = sys.argv[0]
	if( len(msg) > 0 ):
		print "%s: %s" % (prog, msg)
	print "Usage: %s [-vs] [file]" % (prog)
	print "\t-v, --verbose\t\t Print verbose output"
	print "\t-s, --separator\t\t Set field separator"
	if(msg == "Help"):
		print ""
		print "This program will create an sqlite database from the contents of the given file."
		print "The database file will be named the same as the given file plus the extension '.sqlite'."
		print "The table will also be named the same as the given filename."
		print "Column names are taken from the first row in the file."
		print ""


if __name__ == '__main__':
	main()
