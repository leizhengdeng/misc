#!/usr/bin/env python

#to run "qsub_cmd.py" on the main node
#(1.a) qsub_cmd.py B6312.cmd.txt -p ::walltime=5:00:00 1>out.log 2>err.log
#(1.b) nohup qsub_cmd.py B6312.cmd.txt -p ::walltime=5:00:00 1>out.log 2>err.log &


# or run "qsub_cmd.py" on the cluster node:
#(2.a) q qsub_cmd.py B6312.cmd.txt -p ::walltime=8:00:00 
#(2.b) echo "qsub_cmd.py B6312.cmd.txt -p ::walltime=8:00:00 1>out.log 2>err.log"|qsub -l walltime=24:00:00


#Example: python qsub_cmd.py pipeline_commands.sh -s 1-3 -p 2::mem=99gb -b /cri/home2/zlei2/pbs 1>qsub_cmd.out 2>qsub_cmd.err |qsub
#http://tiku.io/questions/4161253/how-can-i-use-a-pipe-or-redirect-in-a-qsub-command

import sys
import os
import re
import argparse
from pbs import PBS
#from optparse import OptionParser

class GLOBAL:
	MAX_STEP = 0
	arg_parser = None

MAX_STEP = 0
arg_parser = None

def main():
	global MAX_STEP
	global arg_parser
	print ' '.join(sys.argv)
	args = get_arguments()
	step2cmds = parse_cmd_file(args.cmd_file)
	#print step2cmds
	#for k,v in step2cmds.items():
	#	print k, ":", v, "\n"		

	cmd_filename = os.path.basename(args.cmd_file)
	prefix = os.path.splitext(cmd_filename)[0]

	#print "Max step:", MAX_STEP
	run_step_list = parse_step(args.step)
	#print run_step_list
	#sys.exit()

	[pbs_setting_step_list, pbs_params] = parse_pbs_setting(args.pbs_setting)
	print "pbs_setting_step_list:", pbs_setting_step_list
	print "pbs_params: ", pbs_params

	#for [k,v] in step2cmds.items():
	#	print k
	#	for a in v:
	#		print a	
	#sys.exit()
	mypbs = PBS(base_dir = args.base_dir, prefix = prefix)
	print "======================================================"
	print ' '.join(sys.argv)
	print "PBS setting when it starts running the commands:"
	mypbs.print_default_setting()
	print "======================================================\n"
	mypbs.write_default_log(' '.join(sys.argv))

	for step in range(1, MAX_STEP+1):
		if step in run_step_list:
			if  (pbs_setting_step_list is not None and step in pbs_setting_step_list):
				mypbs.set_current_pbs_params(pbs_params)
			else:
				mypbs.set_current_pbs_params()
						
			print "\n========\nSTEP:" + str(step)
			#print mypbs.get_current_pbs_params()

			jobname_list = []
			cmd_list = step2cmds[step]
			len_max_step = len(str(MAX_STEP))
			str_step = prefix + ".STEP" + str(step).zfill(len_max_step)
			for subjob in range(1, len(cmd_list)+1):
				len_cmd_list = len(str(len(cmd_list)))
				jobname_list.append(str_step + '-' + str(subjob).zfill(len_cmd_list))
			
			#for i in range(0, len(cmd_list)):
			#	print jobname_list[i]
			#	print cmd_list[i]
			mypbs.run_jobs(jobname_list, cmd_list)

			print "========\n" 

	mypbs.close()

	sys.exit()

	mypbs.set_current_pbs_params("mem=1gb,walltime=1:00:00")
	mypbs.print_setting()
	mypbs.run_cmd("ls -ltr ", "mycmd")
	mypbs.run_cmd(["wc -l ~/ ", "wc -l *"], "CMD")
	mypbs.close()







def parse_cmd_file(cmd_file):
	global MAX_STEP
	global arg_parser
	step2cmds = {}

	cmd_fh = open(cmd_file)
	content = cmd_fh.readlines()

	#remove all lines starts with "#"
	content = [x for x in content if not x.startswith("#")] 

	#concatenate the command lines ends with \
	str_content = "".join(content)
	str_content = str_content.replace("\r", "")
	str_content = str_content.replace("\\\n", "")
	content = str_content.split("\n")

	current_step = 1
	previous_line_is_empty = True
	for line in content:
		line = line.strip()
		if (line == "" and previous_line_is_empty):
			previous_line_is_empty = True
			continue
		else:
			previous_line_is_empty = False
		if step2cmds.has_key(current_step):
			if (not (line == "")):
				step2cmds[current_step].append(line.rstrip(os.linesep))
		else:
			if (not (line == "")):
				step2cmds[current_step] = [line.rstrip(os.linesep)]
		if (line == ""):
			current_step = current_step + 1
			previous_line_is_empty = True
	cmd_fh.close()
	if (not step2cmds):
		arg_parser.print_help()
		print "\n\nError: " + cmd_file + " contains no commands!\n"
		sys.exit()
	else:
		MAX_STEP = len(step2cmds.keys())
	return step2cmds






def parse_step(step):
	global MAX_STEP
	global arg_parser
	step_list = []
	if step is None or step == "":
		step_list = range(1, MAX_STEP+1)
	else:
		for step_range in step.split(','):
			if ('-' in step_range):
				[step_from, step_to] = step_range.split('-')
				if (step_from == ""):
					step_from = 1
				if (step_to == ""):
					step_to = MAX_STEP
			else:
				step_from = step_range
				step_to = step_range
			try:
				step_from = int(step_from)
				step_to = int(step_to)
			except Exception as ex:
				arg_parser.print_help()
				print "\n\nError: argument \"-s " + step + "\" is not valid STEP format!\n"
				sys.exit()
			for i in range(step_from, step_to+1):
				step_list.append(i)
	return step_list




def parse_pbs_setting(pbs_setting):
	pbs_setting_step_list = []
	pbs_params = ""

	if pbs_setting is None or pbs_setting == "":
		return [None, None]

	if ("::" in pbs_setting):
		[str_step_list,  pbs_params] = pbs_setting.split("::")
		pbs_setting_step_list = parse_step(str_step_list)

	else:
		str_step_list = ""
		pbs_setting_step_list = parse_step(str_step_list)
		pbs_params = pbs_setting
	return [pbs_setting_step_list, pbs_params]

	
			
def get_arguments():
	global MAX_STEP
	global arg_parser
	main_description = '''\
	Run command lines using PBS.
	Author: Zhengdeng Lei (zlei2@uic.edu)

	'''
	help_help = '''\
	show this help message and exit\
	'''
	version_help = '''\
	show the version of this program\
	'''
	cmd_file_help = '''\
	command file (e.g. pipeline_commands.sh).
	Steps are separated by an empty line. 
	Command lines within each step will be submitted simultaneously.
	'''
	step_help = '''\
	range of steps in commands, default all steps. e.g. 
	(-s 1-5,8,11-)\trun step 1 to 5, 8, 11 and all remaining steps.
	'''
	pbs_setting_help = '''\
	PBS setting for range of steps. e.g. 
	(-p 7,8::mem=50gb,ppn=12,walltime=24:00:00):
	for step 7 and 8, PBS will use memory 50gb, 12 processors, and walltime 24 hours.
	'''
	base_dir_help = '''\
	the project's base directory, default current directory, e.g. 
	(-b /cri/home2/zlei2/McLachlan)
	all *.sh, *.out, *.err, *.log files will be save at base_dir/qsub_cmd_log.
	'''

	arg_parser = argparse.ArgumentParser(description=main_description, formatter_class=argparse.RawTextHelpFormatter, add_help=False)

	###############################
	#    1. required arguments    #
	###############################
	required_group = arg_parser.add_argument_group("required arguments")

	###############################
	#    2. optional arguments    #
	###############################
	optional_group = arg_parser.add_argument_group("optional arguments")
	optional_group.add_argument("-s", dest="step", default=None, help=step_help)
	optional_group.add_argument("-p", dest="pbs_setting", default=None, help=pbs_setting_help)
	optional_group.add_argument("-b", dest="base_dir", default=os.getcwd(), help=base_dir_help)



	optional_group.add_argument("-h", "--help", action="help", help=help_help)
	optional_group.add_argument("-v", "--version", action="version", version="%(prog)s: version 1.0", help=version_help)

	###############################
	#    3. positional arguments  #
	###############################
	arg_parser.add_argument('cmd_file', metavar="COMMAND_FILE", type=str, help=cmd_file_help)


	args = arg_parser.parse_args()
	args.cmd_file = os.path.abspath(args.cmd_file)
	args.base_dir = os.path.abspath(args.base_dir)

	create_dirs = ["base_dir"]
	for create_dir in create_dirs:
		dir_path = eval("args." + create_dir)
		if not os.path.exists(dir_path):
			print dir_path, "is created!\n"
			os.makedirs(dir_path)

	must_exist_paths = ["cmd_file"]
	for must_exist_path in must_exist_paths:
		path = eval("args." + must_exist_path)
		if not os.path.exists(path):
			arg_parser.print_help()
			print "\n\nError: " + path + " does not exist!\n"
			sys.exit()

	return args
	#args = get_arguments()
	#return [args, arg_parser]
	#[args, arg_parser] = get_arguments()


	
if __name__ == '__main__':
	main()


