#!/usr/bin/env python
import sys
import os
import re
import argparse
from Bio import Entrez, SeqIO
from os.path import expanduser
import time


			
def main():
	N_network_try = 5
	fs	= FS()
	u	= UTILITIES()
	required_tools = ["NormalizeFasta.jar"]
	for tool in required_tools:
		if not tool in u.tool2path:
			print "Error:", tool, "not found in ~/UTILITIES"
			sys.exit()

	#args has members: out_dir, genomes
	args = get_arguments()
	Entrez.email	= "none@uic.edu"             # Always tell NCBI who you are

	for genomeAccession in args.genomes:
		genomeAccession = genomeAccession.split(".")[0] # + " complete genome"
		search		= genomeAccession
		for try_times in range(1,N_network_try):
			try:
				print "searching", search
				handle	= Entrez.read(Entrez.esearch(db="nucleotide", property='complete genome', term=search, retmode="xml"))
				break
			except:
				print "Network exception found, try again!\n"
				time.sleep(2)
				continue
		GIs	= handle['IdList']
		if not GIs:
			print "No genome found:", search
			sys.exit()
		#print GIs
		#print "***********"
		###############################
		# Generate Genome Fasta files #
		###############################
		GI = GIs[0]
		fasta_file_raw	= args.out_dir + os.path.sep + genomeAccession + ".raw.fasta"
		fasta_file_norm	= fasta_file_raw[:-10]+".norm.fasta"
		fasta_file	= fasta_file_raw[:-10]+".fasta"


		if os.path.exists(fasta_file):
			print fasta_file, "already exist!\n"
			continue


		for try_times in range(1,N_network_try):
			try:
				handle = Entrez.efetch(db="nucleotide", id=GI, rettype="fasta", strand=1, retmode="text")
				break
			except:
				print "Network exception found, try again!\n"
				time.sleep(2)
				continue


		record = SeqIO.read(handle, "fasta")
		if not record:
			print "No fasta found for:", search
			sys.exit()
		#print record.format("fasta")[:200]
		fs.write(fasta_file_raw, record.format("fasta"))
		fs.close()
		print fasta_file_raw, "created!\n"

		#cmd1 = "java -jar %s I=%s O=%s 2&>1 >/dev/null" % (u.tool2path["NormalizeFasta.jar"], fasta_file_raw, fasta_file)
		cmd1 = "java -jar %s I=%s O=%s" % (u.tool2path["NormalizeFasta.jar"], fasta_file_raw, fasta_file_norm)
		os.system(cmd1)

		#cmd1 = "rm %s" % (fasta_file_raw)
		#os.system(cmd1)
		#print "Normalized genome", fasta_file_raw, "is created!\n"

		fs.write(fasta_file, ">" + genomeAccession + "\n")
		for line in fs.readSkipHeader(fasta_file_norm):
			fs.write(fasta_file, line)
		fs.close()
		print fasta_file, "created!\n"

		cmd1 = "rm %s" % (fasta_file_norm)
		os.system(cmd1)

		cmd1 = "head %s %s" % (fasta_file_raw, fasta_file)
		os.system(cmd1)





	sys.exit()

			
def get_arguments():
	main_description = '''\
	NOTE: This program uses NCBI keyword search function, therefore please always double check by 
	checking the header of the downloaded genome file (*.raw.fasta)

	Downlod reference genome(s) from the web using BioPython.
	Normalize the reference genome using Picard NormalizeFasta.
	Rename the genome name in the fasta header line in order to match snpEff database.
	(assume the chromosome name in snpEffectPredictor.bin is CHROMOSOME)
	Author: Zhengdeng Lei (zlei2@uic.edu)
	'''
	epi_log='''
This program requires:
1. Recommend to run on hebe
2. BioPython
3. NormalizeFasta.jar (Picard) in ~/UTILITIES
	'''

	help_help = '''\
	show this help message and exit\
	'''
	version_help = '''\
	show the version of this program\
	'''

	out_dir_help = '''\
	output directory 
	e.g. the project directory for SPANDx.
	(default the current directory)
	\
	'''

	genome_help = '''\
	list of genome accession numbers.
	e.g. -g NC_010079
	e.g. -g NC_010079 -g AP008954\
	'''

	arg_parser = argparse.ArgumentParser(description=main_description, epilog=epi_log, formatter_class=argparse.RawTextHelpFormatter, add_help=False)

	###############################
	#    1. required arguments    #
	###############################
	required_group = arg_parser.add_argument_group("required arguments")
	required_group.add_argument("-g", dest="genomes", metavar="GENOME LIST", action="append", required=True, default=[], help=genome_help)

	###############################
	#    2. optional arguments    #
	###############################
	optional_group = arg_parser.add_argument_group("optional arguments")
	optional_group.add_argument("-o", dest="out_dir", default=os.getcwd(), help=out_dir_help)

	optional_group.add_argument("-h", "--help", action="help", help=help_help)
	optional_group.add_argument("-v", "--version", action="version", version="%(prog)s: version 1.0", help=version_help)

	###############################
	#    3. positional arguments  #
	###############################
	#arg_parser.add_argument('command_file', metavar="COMMAND_FILE", type=str, help=command_file_help)

	args = arg_parser.parse_args()
	
	#for required_arg in required_args:
	#	if not eval("args." + required_arg):
	#		arg_parser.print_help()
	#		print "\n\nError: ", required_arg.upper(), "is required!\n"
	#		sys.exit()
	create_dirs = ["out_dir"]
	for create_dir in create_dirs:
		dir_path = eval("args." + create_dir)
		if not os.path.exists(dir_path):
			print dir_path, "is created!\n"
			os.makedirs(dir_path)
	return args
	#args = get_arguments()

	#return [args, arg_parser]
	#[args, arg_parser] = get_arguments()



class FS:
	def __init__(self):
		self.filename2handle = {}
	
	def read(self, filename):
		fh = open(filename)
		if filename in self.filename2handle:
			fh = self.filename2handle[filename]
		else:
			fh = open(filename)
			self.filename2handle[filename] = fh
		return fh


	def readHeader(self, filename):
		fh = open(filename)
		if filename in self.filename2handle:
			fh = self.filename2handle[filename]
		else:
			fh = open(filename)
			self.filename2handle[filename] = fh
		return fh.next()
	
	def readSkipHeader(self, filename):
		if filename in self.filename2handle:
			fh = self.filename2handle[filename]
		else:
			fh = open(filename)
			fh.next()
			self.filename2handle[filename] = fh
		return fh

	def readSkipLines(self, filename, N_lines=1):
		if filename in self.filename2handle:
			fh = self.filename2handle[filename]
		else:
			fh = open(filename)
			for i in xrange(N_lines):
				fh.next()
			self.filename2handle[filename] = fh
		return fh
	#To read large file line by line
	#fh = open("infile.txt")
	#header = fh.next()
	#for line in fh:
	#	sample_list.append(line[:10].rstrip())	
	#fh.close()

	def write(self, filename, str_buffer):
		if filename in self.filename2handle:
			#print "existing file"
			fh = self.filename2handle[filename]
		else:
			#print "new file"
			fh = open(filename, "wt")
			self.filename2handle[filename] = fh
		fh.write(str_buffer)

	def close(self, filename=None):
		"""if filename is None, then close all files; otherwise only close the single file"""
		if not filename is None:
			if filename in self.filename2handle:
				self.filename2handle[filename].close()
				del self.filename2handle[filename]
				#print "[CLOSE]", filename
			else:
				print "No file handle found for", filename
		else:
			for filename, fh in self.filename2handle.items():
				fh.close()
				del self.filename2handle[filename]
				#print "[CLOSE]", filename
			

class UTILITIES:
	def __init__(self):
		self.tool2path = {}
		home_dir = expanduser("~")
		u_fh = open(home_dir + os.path.sep + "UTILITIES")
		for line in u_fh:
			if not line.startswith("#"):
				tool = line.split()
				if len(tool) >= 2:
					#tool[0] is the tool name, tool[1] is the tool path
					self.tool2path[tool[0]] = tool[1]
		u_fh.close()
	
	 
if __name__ == '__main__':
	main()


