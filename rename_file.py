#!/usr/bin/env python
#convert DOS newlines (CR/LF) to Unix format.
#sed 's/.$//'
import sys
import os
import re
import datetime
import logging
import argparse
if not "/cri/home2/zlei2/python_lib" in sys.path: sys.path.append("/cri/home2/zlei2/python_lib")
from fs import FS

log_level = logging.DEBUG
logging.basicConfig(filename=None, format='%(asctime)s [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=log_level)

#Example:
# rename_file.py -s ./ -r n -f .py -re $:.jpg add .jpg to the filename
# rename_file.py -s ../rawdata -re _[ACGT]{8}.*_001: -m map.txt



def main():
	fs = FS()
	args = get_arguments()
	#print args

	src_files = []
	if args.recursive:
		for dirpath, dirnames, filenames in os.walk(args.source_dir):
			for filename in filenames:
				if args.file_type is None:
					src_files.append(os.path.join(dirpath, filename))
				elif filename.endswith(args.file_type):
					src_files.append(os.path.join(dirpath, filename))
	else:
		for filename in next(os.walk(args.source_dir))[2]:
			if args.file_type is None:
				src_files.append(os.path.join(args.source_dir, filename))
			elif filename.endswith(args.file_type):
				src_files.append(os.path.join(args.source_dir, filename))

	cmds = []
	dest_files = []

	if not args.mapping_file is None:
		src2dest = {}
		content = fs.read2list(args.mapping_file)
		fs.close()

		#if args.mapping_header:
		for i in xrange(0, len(content)):
			try:
				(src_filename, dest_filename) = content[i].split()
				src2dest[src_filename] = dest_filename
			except Exception as ex:
				logging.warning("mapping src file to dest file [empty line?]: " + str(ex))
	src_file2dest_file = {}
	for src_file in src_files:
		src_filename = os.path.basename(src_file)
		dest_filename = src_filename
		if not args.re_list is None:
			for regular_expression in args.re_list:
				(find_str, replace_str) = regular_expression.split(":")
				print find_str, "be replaced with:", replace_str
				dest_filename = re.sub(find_str, replace_str, dest_filename)
			dest_file = os.path.join(args.out_dir, dest_filename)
			src_file2dest_file[src_file] = dest_file

		elif not args.mapping_file is None:
			if not src_filename in src2dest:
				logging.warning("\"%s\" is NOT listed in the mapping file!!!\n" % src_filename )
			else:
				dest_file = os.path.join(args.out_dir, src2dest[src_filename])
				src_file2dest_file[src_file] = dest_file
		else:
			dest_file = os.path.join(args.out_dir, src_filename)
		src_file2dest_file[src_file] = dest_file
			

	
	for (src_file, dest_file) in sorted(src_file2dest_file.items()):
		if not os.path.exists(src_file):
			logging.error("\"%s\" does not exist!!!\n" % src_file )
			sys.exit()
		if args.hard_copy:
			cmd_line = "cp '%s' '%s'" % (src_file, dest_file)
		else:
			cmd_line = "ln -s %s %s" % (src_file, dest_file)
		logging.info(cmd_line)
		#print "\n"
		os.system(cmd_line)
		
	




def get_arguments():
	main_description = '''\
Rename files by coping or symoblic link.
Author: Zhengdeng Lei (zlei2@uic.edu)


	'''
	epi_log='''
This program requires the python packages:
1. argparse
2. openpyxl (pypm install "openpyxl<=2.1.5")
3. logging
	'''	
	help_help = '''\
	show this help message and exit\
	'''
	version_help = '''\
	show the version of this program\
	'''
	#command_file_help = '''\
	#pipeline command file (e.g. pipeline_commands.sh).
	#Steps are separated by an empty line. 
	#Command lines within each step will be run simultaneously.
	#'''

	source_dir_help = '''\
	The source dictionary that contains the source files.
	'''

	recursive_help = '''\
	Recursively search for files in a directory.
	y - recursively search
	n - source directory only
	default is n.
	'''

	file_type_help = '''\
	specify the file type
	'''
	out_dir_help = '''\
	out_dir_help
	default is None.
	'''
	hard_copy_help = '''\
	hard copy or not
	y - copy the files from source to destination
	n - just make the symbolic link
	default is n.
	'''
	
	mapping_file_help = '''\
	mapping_file_help
	default is 1.
	'''
	regular_expression_list_help = '''\
	list of regular expression for renaming the files.
	e.g. -re Rush-: -re R1_0001:R1
	e.g. source file is Rush-H3-1_S77_R1_0001.fastq, 
	then destination file is H3-1_S77_R1.fastq
	default is None.
	'''

	
	arg_parser = argparse.ArgumentParser(description=main_description, epilog=epi_log, formatter_class=argparse.RawTextHelpFormatter, add_help=False)
	arg_parser.register('type','bool', lambda s: str(s).lower() in ['true', '1', 't', 'y', 'yes']) # add type keyword to registries
	###############################
	#    1. required arguments    #
	###############################
	required_group = arg_parser.add_argument_group("required arguments")
	required_group.add_argument("-s", dest="source_dir", action="store", required=True, default=None, help=source_dir_help)

	###############################
	#    2. optional arguments    #
	###############################
	optional_group = arg_parser.add_argument_group("optional arguments")
	optional_group.add_argument("-r", dest="recursive", type='bool', default=False, help=recursive_help)
	optional_group.add_argument("-f", dest="file_type", default=None, help=file_type_help)
	optional_group.add_argument("-o", dest="out_dir", default=os.getcwd(), help=out_dir_help)
	optional_group.add_argument("-d", dest="hard_copy", type='bool', default=False, help=hard_copy_help)
	optional_group.add_argument("-m", dest="mapping_file", default=None, help=mapping_file_help)
	optional_group.add_argument("-re", dest="re_list", metavar="REGULAR EXPRESSION LIST", action="append", default=None, help=regular_expression_list_help)

	###############################
	#    3. positional arguments  #
	###############################
	#arg_parser.add_argument('command_file', metavar="COMMAND_FILE", type=str, help=command_file_help)


	optional_group.add_argument("-h", "--help", action="help", help=help_help)
	optional_group.add_argument("-v", "--version", action="version", version="%(prog)s: version 1.0", help=version_help)
	args = arg_parser.parse_args()

	args.out_dir = os.path.abspath(args.out_dir)
	args.source_dir = os.path.abspath(args.source_dir)
	
	#crrt_time = datetime.datetime.now()
	#crrt_time = "%4d-%02d-%02d_%02d-%02d-%02d" % (crrt_time.year, crrt_time.month, crrt_time.day, crrt_time.hour, crrt_time.minute, crrt_time.second)
	create_dirs = ["out_dir"]
	for create_dir in create_dirs:
		dir_path = eval("args." + create_dir)
		if not os.path.exists(dir_path):
			os.makedirs(dir_path)
			logging.debug("%s is created!\n" % dir_path)



	if (not args.mapping_file is None) and (not args.regular_expression is None):
		logging.error("Only one of arguments -m and -re is accepted, but NOT both!!!\n" )
		sys.exit()


	must_exist_paths = ["source_dir"]
	for must_exist_path in must_exist_paths:
		path = eval("args." + must_exist_path)
		if not os.path.exists(path):
			arg_parser.print_help()
			logging.error("\"%s\" does not exist!!!\n" % path )
			sys.exit()


	#if not os.path.dirname(args.excel_out):
	#	args.excel_out = os.path.join(os.getcwd(),args.excel_out)
	return args

if __name__ == '__main__':
	main()







