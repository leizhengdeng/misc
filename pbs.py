import os
import uuid
import time
import re
import datetime
import subprocess

class PBS:
	default_pbs_params = {"email":"", "mail_opt":"", "joblog":"", "jobname":"", "mem":"16gb", "nodes":"1",  "ppn":"8", "q":"", "walltime":"20:00:00"}
	current_pbs_params = {}
	#resource list
	l_list = ("walltime", "nodes", "ppn", "gpus", "mem", "software", "file")
	m_list = ("mail_opt")
	M_list = ("email")
	N_list = ("jobname")
	j_list = ("joblog")
	q_list = ("q")

	uuid2jobid = {}
	job_unique_id2jobid = {} # e.g. job_unique_id like Step3-1.4bc7a0b8-d319-4635-afde-d6957ae05317
	jobid2cmd = {}		 # e.g. jobid 19526
	base_dir = ""
	qsub_cmd_dir = ""
	qsub_cmd_file = ""
	qsub_cmd = None 
	err_file_list = {}
	num_of_jobs = 0


	def __init__(self, pbs_params=None, base_dir=None, prefix = ""):
		if (pbs_params is not None):
			params = pbs_params.split(",")
			for param in params:
				[param_k, param_v] = param.split("=")
				self.default_pbs_params[param_k] = param_v
		if (base_dir is None):
			base_dir = os.getcwd()
		if (base_dir.endswith(os.path.sep)):
			base_dir = base_dir[:-1]
		self.base_dir = base_dir
		self.qsub_cmd_dir = base_dir + os.path.sep + "qsub_cmd_log"
		if not os.path.exists(self.qsub_cmd_dir):
			os.makedirs(self.qsub_cmd_dir)
		crrt_time = datetime.datetime.now()
		crrt_time = "%4d%02d%02d_%02d%02d%02d" % (crrt_time.year, crrt_time.month, crrt_time.day, crrt_time.hour, crrt_time.minute, crrt_time.second)
		self.qsub_cmd_file = self.qsub_cmd_dir + os.path.sep + prefix + "_qsub_cmd." + crrt_time + ".log"
		self.qsub_cmd = open(self.qsub_cmd_file, "wt")
 		self.current_pbs_params = self.default_pbs_params.copy()


	def get_default_pbs_params(self):
		return self.default_pbs_params

	def get_current_pbs_params(self):
		return self.current_pbs_params

	def set_current_pbs_params(self, pbs_params=None):
		if (pbs_params is None):
			self.current_pbs_params = self.default_pbs_params
		else:
			self.current_pbs_params = self.default_pbs_params.copy()
			params = pbs_params.split(",")
			for param in params:
				[param_k, param_v] = param.split("=")
				self.current_pbs_params[param_k] = param_v			


	def print_default_setting(self):
		for k in sorted(self.default_pbs_params.keys()):
			if (self.default_pbs_params[k] != ""):			
				if k in self.l_list:
					print k + " = " + self.default_pbs_params[k]
				elif k in self.m_list:
					print k + " = " + self.default_pbs_params[k]
				elif k in self.M_list:
					print k + " = " + self.default_pbs_params[k]
				elif k in self.N_list:
					print k + " = " + self.default_pbs_params[k]
				elif k in self.j_list:
					print k + " = " + self.default_pbs_params[k]
				elif k in self.q_list:
					print k + " = " + self.default_pbs_params[k]
				else:
					print k + " = " + self.default_pbs_params[k]
		print "base dir is: " + self.base_dir
		print "pipeline log file is: " + self.qsub_cmd_file


	def print_current_setting(self):
		for k in sorted(self.current_pbs_params.keys()):
			if (self.current_pbs_params[k] != ""):			
				if k in self.l_list:
					print k + " = " + self.current_pbs_params[k]
				elif k in self.m_list:
					print k + " = " + self.current_pbs_params[k]
				elif k in self.M_list:
					print k + " = " + self.current_pbs_params[k]
				elif k in self.N_list:
					print k + " = " + self.current_pbs_params[k]
				elif k in self.j_list:
					print k + " = " + self.current_pbs_params[k]
				elif k in self.q_list:
					print k + " = " + self.current_pbs_params[k]
				else:
					print k + " = " + self.current_pbs_params[k]
		print "base dir is: " + self.base_dir
		print "pipeline log file is: " + self.qsub_cmd_file




	def run_jobs(self, jobname_list, cmd_list):
		self.job_unique_id2jobid = {}
		self.jobid2cmd = {}
		self.num_of_jobs = 0


		crrt_time = datetime.datetime.now()
		crrt_time = "%4d-%02d-%02d %02d:%02d:%02d" % (crrt_time.year, crrt_time.month, crrt_time.day, crrt_time.hour, crrt_time.minute, crrt_time.second)
		#self.qsub_cmd.write(crrt_time + "\tJOB_SUMMIT\t" + cmd + os.linesep)
		self.qsub_cmd.write(crrt_time + "\t" + jobname_list[0].split('-')[0].upper() + os.linesep)


		#if (type(cmd) is str):
		#	self.num_of_jobs = 1
		#	self.default_pbs_params["jobname"] = cmd_label
		#	self.write_and_run_script(cmd)
		#else:
		for i in range(len(cmd_list)):
			if (cmd_list[i] and (not cmd_list[i].isspace())):
				self.num_of_jobs = self.num_of_jobs + 1
				self.write_and_run_script(jobname_list[i], cmd_list[i])
				print cmd_list[i], " is submitted!"
		self.check_if_all_jobs_executed()
		self.check_if_all_jobs_finished()
		

	def write_and_run_script(self, jobname, cmd):
		self.current_pbs_params["jobname"] = jobname
		cmd = cmd.rstrip()
		job_uuid = str(uuid.uuid4())
		job_unique_id = jobname + "." + job_uuid

		sh_file_name	= job_unique_id + ".sh"
		jobid_file_name	= job_unique_id + ".jobid"
		out_file_name	= job_unique_id + ".out"
		err_file_name	= job_unique_id + ".err"

		#self.err_file_list[self.qsub_cmd_dir + os.path.sep + self.current_pbs_params["jobname"] + "." + job_uuid + ".err"] = cmd
		sh_file = open(self.qsub_cmd_dir + os.path.sep + sh_file_name, "wt")
		sh_file.write("#!/bin/sh" + os.linesep)
		for k in sorted(self.current_pbs_params.keys()):
			if (self.current_pbs_params[k] != ""):
				if (k == "nodes"):
					sh_file.write("#PBS -l " + k + "=" + self.current_pbs_params[k] + ":ppn=" + self.current_pbs_params["ppn"] + os.linesep)
					continue
				if (k == "ppn"):
					continue	
				if k in self.l_list:
					sh_file.write("#PBS -l " + k + "=" + self.current_pbs_params[k] + os.linesep)
				elif k in self.m_list:
					sh_file.write("#PBS -m " + self.current_pbs_params[k] + os.linesep)
				elif k in self.M_list:
					sh_file.write("#PBS -M " + self.current_pbs_params[k] + os.linesep)
				elif k in self.N_list:
					sh_file.write("#PBS -N " + self.current_pbs_params[k] + os.linesep)
				elif k in self.j_list:
					sh_file.write("#PBS -j " + self.current_pbs_params[k] + os.linesep)
				elif k in self.q_list:
					sh_file.write("#PBS -q " + self.current_pbs_params[k] + os.linesep)
				else:
					sh_file.write("#PBS -l " + k + "=" + self.current_pbs_params[k] + os.linesep)
		sh_file.write("#PBS -o " + self.qsub_cmd_dir + os.path.sep + out_file_name + os.linesep)
		sh_file.write("#PBS -e " + self.qsub_cmd_dir + os.path.sep + err_file_name + os.linesep)

		sh_file.write(os.linesep)
		sh_file.write("cd " + self.qsub_cmd_dir + os.linesep)
		sh_file.write("echo $PBS_JOBID >" + jobid_file_name + os.linesep)
		#sh_file.write(cmd + " 1>" + self.qsub_cmd_dir + os.path.sep + out_file_name + " 2>" + self.qsub_cmd_dir + os.path.sep + err_file_name + os.linesep)
		sh_file.write("cd " + self.base_dir + os.linesep)
		sh_file.write(cmd + os.linesep)
		sh_file.close()

		qsub_cmd = "qsub " + self.qsub_cmd_dir + os.path.sep + sh_file_name
		p = subprocess.Popen(qsub_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		[output, errmsg] = p.communicate()
		#if p.returncode != 0:
		#	raise OSError(p.returncode, "command %r failed: %s" % (cmd, errmsg))
		#print output
		jobid = output.split('.')[0]
		#job_id = re.match("(\d+)", output).group(1)
		crrt_time = datetime.datetime.now()
		crrt_time = "%4d-%02d-%02d %02d:%02d:%02d" % (crrt_time.year, crrt_time.month, crrt_time.day, crrt_time.hour, crrt_time.minute, crrt_time.second)
		#self.qsub_cmd.write(crrt_time + "\tJOB_SUMMIT\t" + cmd + os.linesep)
		self.qsub_cmd.write(crrt_time + "\tJOB_SUMMIT(" + jobid + ")\t" + cmd + os.linesep)
		self.job_unique_id2jobid[job_unique_id] = jobid
		self.jobid2cmd[jobid] = cmd
		


	def check_if_all_jobs_executed(self):
		num_of_job_execuated = 0
		job_unique_id2jobid = self.job_unique_id2jobid.copy()
		#print "number of jobs waiting to be run " + str(len(job_unique_id2jobid.keys()))
		while True:
			for job_unique_id in job_unique_id2jobid.keys():
				if (os.path.exists(self.qsub_cmd_dir + os.path.sep + job_unique_id + ".jobid")):
					jobid = job_unique_id2jobid[job_unique_id]
					num_of_job_execuated = num_of_job_execuated + 1
					crrt_time = datetime.datetime.now()
					crrt_time = "%4d-%02d-%02d %02d:%02d:%02d" % (crrt_time.year, crrt_time.month, crrt_time.day, crrt_time.hour, crrt_time.minute, crrt_time.second)
					self.qsub_cmd.write(crrt_time + "\tJOB_RUN   (" + jobid + ")\t" + self.jobid2cmd[jobid] + os.linesep)
					del job_unique_id2jobid[job_unique_id]

			#print "number of jobs waiting to be run " + str(len(job_unique_id2jobid.keys()))

			if (len(job_unique_id2jobid.keys()) == 0):
				break
			else:
				time.sleep(2)
	
	def check_if_all_jobs_finished(self):
		'''	0 - all jobs are complete and no error found
			1 - at least error found for one job
			2 - job is running
		'''
		jobid2cmd = self.jobid2cmd.copy()
		#print "number of jobs waiting to be finished " + str(len(jobid2cmd.keys()))
		while True:
			for jobid in jobid2cmd.keys():
				qstat_cmd = "qstat -f " + jobid + "|grep state"
				p = subprocess.Popen(qstat_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				[output, errmsg] = p.communicate()
				#if p.returncode != 0:
				#	raise OSError(p.returncode, "command %r failed: %s" % (cmd, errmsg))
				#print output
				job_state = output.split('=')[1].strip()
				#print "job state =;;;;;" + job_state + ";;;;;"

				if (job_state == "C" or job_state == "E"):
					crrt_time = datetime.datetime.now()
					crrt_time = "%4d-%02d-%02d %02d:%02d:%02d" % (crrt_time.year, crrt_time.month, crrt_time.day, crrt_time.hour, crrt_time.minute, crrt_time.second)
					self.qsub_cmd.write(crrt_time + "\tJOB_FINISH(" + jobid + ")\t" + self.jobid2cmd[jobid] + os.linesep)
					del jobid2cmd[jobid]

			#print "number of jobs waiting to be run " + str(len(jobid2cmd.keys()))

			if (len(jobid2cmd.keys()) == 0):
				break
			else:
				time.sleep(2)
		time.sleep(1)
		print "Jobs are finished for current step"
		

		
	def write_default_log(self, str_log):
		self.qsub_cmd.write("======================================================" + os.linesep)
		self.qsub_cmd.write(str_log + os.linesep)
		self.qsub_cmd.write("PBS setting when it starts running the commands:" + os.linesep)
		for k in sorted(self.default_pbs_params.keys()):
			if (self.default_pbs_params[k] != ""):			
				if k in self.l_list:
					self.qsub_cmd.write(k + " = " + self.default_pbs_params[k] + os.linesep)
				elif k in self.m_list:
					self.qsub_cmd.write(k + " = " + self.default_pbs_params[k] + os.linesep)
				elif k in self.M_list:
					self.qsub_cmd.write(k + " = " + self.default_pbs_params[k] + os.linesep)
				elif k in self.N_list:
					self.qsub_cmd.write(k + " = " + self.default_pbs_params[k] + os.linesep)
				elif k in self.j_list:
					self.qsub_cmd.write(k + " = " + self.default_pbs_params[k] + os.linesep)
				elif k in self.q_list:
					self.qsub_cmd.write(k + " = " + self.default_pbs_params[k] + os.linesep)
				else:
					self.qsub_cmd.write(k + " = " + self.default_pbs_params[k] + os.linesep)
		self.qsub_cmd.write("base dir is: " + self.base_dir + os.linesep)
		self.qsub_cmd.write("pipeline log file is: " + self.qsub_cmd_file + os.linesep)
		self.qsub_cmd.write("======================================================" + os.linesep)

	
	def close(self):
		self.qsub_cmd.close()

