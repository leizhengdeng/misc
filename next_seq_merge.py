#!/usr/bin/env python
import os
import sys
import argparse

python_lib = os.path.expanduser(r"~/python_lib")
if not python_lib in sys.path: sys.path.append(python_lib)
from fs import FS



import subprocess

class ZIP:
	"""run some common command lines"""
	def __init__(self):
		pass
		
	def run(self, command):
		p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		[output, errmsg] = p.communicate()
		if p.returncode == 0: #run successfully
			return output
		else:
			raise OSError(p.returncode, "command %r failed: %s" % (command, errmsg))
			sys.exit()
	
	def test_gz(self, in_gz_list):
		for in_gz in in_gz_list:
			print "Checking", in_gz
			test_gz_cmd = "gunzip -t " + in_gz
			self.run(test_gz_cmd)
			print "PASS"
		
	
	def cat_gz(self, out_gz, in_gz_list):
		cmd1= "cat "
		for in_gz in in_gz_list:
			cmd1 += in_gz + " "
		cmd1 += ">" + out_gz
		return cmd1
			
		
			
		
		
def get_arguments():
	main_description = '''\
	Merge files (e.g. gz files) using patterns (regular expression)
	Author: Zhengdeng Lei (zlei2@uic.edu)

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

	src_dir_help = '''\
	source directory where all fastq files are located in this directory or any sub-directories.
	'''

	fastq_help = '''\
	output files are in fastq format.
	default is n. i.e. output files are in gz format.
	'''

	check_gz_help = '''\
	check the integrity of input gz files.
	default is n. i.e. will check the integrity.
	'''


	arg_parser = argparse.ArgumentParser(description=main_description, formatter_class=argparse.RawTextHelpFormatter, add_help=False)
	arg_parser.register('type','bool', lambda s: str(s).lower() in ['true', '1', 't', 'y', 'yes']) # add type keyword to registries

	###############################
	#    1. required arguments    #
	###############################
	required_group = arg_parser.add_argument_group("required arguments")
	required_group.add_argument("-s", dest="src_dir", action="store", required=True, default=None, help=src_dir_help)

	###############################
	#    2. optional arguments    #
	###############################
	optional_group = arg_parser.add_argument_group("optional arguments")
	optional_group.add_argument("-fq", dest="fastq", type='bool', default=False, help=fastq_help)
	optional_group.add_argument("-c", dest="check_gz", type='bool', default=False, help=check_gz_help)

	optional_group.add_argument("-h", "--help", action="help", help=help_help)
	optional_group.add_argument("-v", "--version", action="version", version="%(prog)s: version 1.0", help=version_help)


	args = arg_parser.parse_args()

	args.src_dir = os.path.abspath(args.src_dir)

	if not os.path.exists(args.src_dir):
		arg_parser.print_help()
		print "\n\nError: source directory does not exist!\n"
		sys.exit()

	return args


def main():
	fs = FS()
	z = ZIP()
	args = get_arguments()
	src_dir = args.src_dir

	next_seq_merge_dir = os.path.join(src_dir, "next_seq_merge")

	if not os.path.exists(next_seq_merge_dir):
		print next_seq_merge_dir, "is created!\n"
		os.makedirs(next_seq_merge_dir)

	next_seq_merge_command_sh = os.path.join(next_seq_merge_dir, "next_seq_merge_commands.sh")

	gz_file_list = []
	for root, dirs, files in os.walk(src_dir, topdown=False):
	    for filename in files:
		if filename.endswith(r".gz"):
			gz_file_list.append(os.path.join(root, filename))

	gz_file_list = sorted(gz_file_list)
	if args.check_gz:
		z.test_gz(gz_file_list)
	#print gz_file_list
	#sys.exit()

	#z.test_gz(gz_file_list)
	#for gz_file in gz_file_list:
	#	test_gz_cmd = "gunzip -t " + gz_file
	#	fs.write(next_seq_merge_command_sh, test_gz_cmd + "\n")
	#fs.write(next_seq_merge_command_sh, test_gz_cmd + "\n\n")
	
	out_gz_list = []	
	for gz_file in sorted(gz_file_list):
		if "_L001_" in gz_file:
			all_lane_file_found = True
			out_gz = os.path.join(next_seq_merge_dir, os.path.basename(gz_file.replace("_L001_", "_")))

			to_cat_list = [gz_file]
			to_cat_list.append(gz_file.replace("_L001_", "_L002_"))
			to_cat_list.append(gz_file.replace("_L001_", "_L003_"))
			to_cat_list.append(gz_file.replace("_L001_", "_L004_"))
			for to_cat_gz in to_cat_list:
				if to_cat_gz not in gz_file_list:
					print "CAN'T find", to_cat_gz
					all_lane_file_found = False
			if all_lane_file_found:
				fs.write(next_seq_merge_command_sh, z.cat_gz(out_gz, to_cat_list) + "\n")
				out_gz_list.append(out_gz)

	fs.write(next_seq_merge_command_sh, "\n\n")	

	if args.fastq:
		for out_gz in out_gz_list:
			fs.write(next_seq_merge_command_sh, "gunzip " + out_gz + "\n")	
		
	fs.write(next_seq_merge_command_sh, "\n\n")	

	qsub_cmd = "nohup qsub_cmd.py " + next_seq_merge_command_sh + " >/dev/null 2>&1 &"
	#print cmd3
	fs.write(next_seq_merge_command_sh, "\n#run this: " + qsub_cmd + "\n")	
	fs.close()

	os.system(qsub_cmd)
		

if __name__ == '__main__':
	main()



	
