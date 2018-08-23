#!/usr/bin/python

#===------------------------------------------------------------*- Python v3 -*-===//
#
#                                     SHAD
#
#      The Scalable High-performance Algorithms and Data Structure Library
#								Performance analyzer
#===----------------------------------------------------------------------===//
#
# Copyright 2018 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
#===----------------------------------------------------------------------===//

# Developer      Date            Description
# ================================================================================
# Methun K       08/03/2018      Created this file
# Methun K       08/07/2018      Created class to modularize the code
# Methun K       08/09/2018      Created class to manage date related operation
# Methun K       08/10/2018      Created data management class that read log files and prepare pandas dataframe
# Methun K       08/13/2018      Created class to manage different kinds of plotting
# Methun K       08/14/2018      Fix the legend position issue, move it to outside the plotting area
# --------------------------------------------------------------------------------

import os, errno
from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np
import math
import matplotlib
from matplotlib import pyplot as plt
import datetime as dt
import sys, getopt
import fileinput

# Class holds datetime functionalities
class SDateTime:
	def __init__(self):
		pass
	def formatDateTime(self, t="", format="%Y-%m-%dT%H:%M:%S.%fZ"):
		if len(t)==0:
			if len(format)==0:
				return str(dt.datetime.now())
			return dt.datetime.strptime(str(dt.datetime.now()),format)

		try:
			return dt.datetime.strptime(str(t),format)
		except:
			return dt.datetime.strptime(str(dt.datetime.now()),format)

# Class holds plotting functionalitites
class SPlot:
	def __init__(self, _dir="/"):
		self.dir = _dir

		if not os.path.exists(self.dir):
			try:
			    os.makedirs(self.dir)
			except OSError as e:
			    if e.errno != errno.EEXIST:
			        raise

	def getFileName(self, xlabel, ylabel):
		fn = ''
		if len(xlabel)>0:
			fn = fn + xlabel.replace(" ", "_") + "_"

		if len(ylabel)>0:
			fn = fn + ylabel.replace(" ", "_") + "_"

		return fn + str(dt.datetime.now()).replace(" ", "_")

	def barPlot(self, x, y, xlabel="", ylabel="", title="", xRot=0, yRot=0, xFontSize=10, yFontSize=10, save=True, fileName="", scaleX="", scaleY="", fdpi=350):
		plt.figure(dpi=fdpi)
		plt.bar(x, y)
		plt.xticks(fontsize=xFontSize)
		plt.yticks(fontsize=yFontSize)

		if scaleX.upper() == "LOG":
			plt.xscale(scaleX)

		if scaleY.upper() == "LOG":
			plt.yscale(scaleY)

		if len(xlabel)>0:
			plt.xlabel(xlabel)

		if xRot!=0:
			plt.xticks(rotation=xRot)

		if len(ylabel)>0:
			plt.ylabel(ylabel)

		if yRot!=0:
			plt.yticks(rotation=yRot)

		if len(title)>0:
			plt.title(title)

		if len(fileName)==0:
			fileName = self.getFileName(xlabel, ylabel)
		
		if save == True:
			plt.savefig(join(self.dir, fileName + ".pdf"), bbox_inches='tight')
			plt.close('all')
		else:
			plt.show()

	def __generateGrid(self, fig, x=[], y=[], showXMajorTics=True, showXMinorTics=True, xMajorTics=10, xMinorTics=5, showYMajorTics=True, showYMinorTics=True, yMajorTics=10, yMinorTics=5, alphaMajor=0.5, alphaMinor=0.2):
		ax = fig.add_subplot(1, 1, 1)

		if len(x) > 0:
			if showXMajorTics==True and xMajorTics!=0:
				ax.set_xticks(np.arange(x[0], x[1], xMajorTics))
			if showXMinorTics==True and xMinorTics!=0:
				ax.set_xticks(np.arange(x[0], x[1], xMinorTics), minor=True)

		if len(y) > 0:
			if showYMajorTics==True and yMajorTics!=0:
				ax.set_yticks(np.arange(y[0], y[1], yMajorTics))
			if showYMinorTics==True and yMinorTics!=0:
				ax.set_yticks(np.arange(y[0], y[1], yMinorTics), minor=True)

		# And a corresponding grid
		ax.grid(which='both')

		# Or if you want different settings for the grids:
		ax.grid(which='minor', alpha=alphaMinor)
		ax.grid(which='major', alpha=alphaMajor)

	'''
	matplotlib.colors
	b : blue.
	g : green.
	r : red.
	c : cyan.
	m : magenta.
	y : yellow.
	k : black.
	w : white.
	'''
	def lineChartPlot(self, x, y, xRange=[], yRange=[], xlabel="", ylabel="", title="", xRot=0, yRot=0, xFontSize=10, yFontSize=10, 
		save=True, fileName="", scaleX="", scaleY="", _showXMajorTics=True, _showXMinorTics=False, _xMajorTics=10, _xMinorTics=5, 
		_showYMajorTics=True, _showYMinorTics=False, _yMajorTics=10, _yMinorTics=5, _alphaMajor=0.5, _alphaMinor=0.2, color=['k'], 
		lineType='-', lineWidth=2, fdpi=350, showGrid=False, showLegends=False, legends=[], legendPos='center left', boxToAnchor=(1, 0.5)):
		
		fig = plt.figure(dpi=fdpi)

		if showGrid==True:
			if len(xRange)==0:
				xRange = [min(x), max(x)]
			if len(yRange)==0:
				yRange = [min(y), max(y)]

			self.__generateGrid(fig, xRange, yRange, showXMajorTics=_showXMajorTics, showXMinorTics=_showXMinorTics, xMajorTics=_xMajorTics, xMinorTics=_xMinorTics, showYMajorTics=_showYMajorTics, showYMinorTics=_showYMinorTics, yMajorTics=_yMajorTics, yMinorTics=_yMinorTics, alphaMajor=_alphaMajor, alphaMinor=_alphaMinor)

		for i in range(0, len(x), 1):
			plt.plot(x[i], y[i], color=color[i], linewidth=lineWidth)
		
		if showLegends==True:
			plt.legend(legends, loc=legendPos, bbox_to_anchor=boxToAnchor)

		plt.xticks(fontsize=xFontSize)
		plt.yticks(fontsize=yFontSize)

		if scaleX.upper() == "LOG":
			plt.xscale(scaleX)

		if scaleY.upper() == "LOG":
			plt.yscale(scaleY)

		if len(xlabel)>0:
			plt.xlabel(xlabel)

		if xRot!=0:
			plt.xticks(rotation=xRot)

		if len(ylabel)>0:
			plt.ylabel(ylabel)

		if yRot!=0:
			plt.yticks(rotation=yRot)

		if len(title)>0:
			plt.title(title)

		if len(fileName)==0:
			fileName = self.getFileName(xlabel, ylabel)
		
		if save == True:
			plt.savefig(join(self.dir, fileName + ".pdf"), bbox_inches='tight')
			plt.close('all')
		else:
			plt.show()


# Data management layer
class SLogAnalysis:
	def __init__(self, _dir="/", _fdir="/"):
		self.dir = _dir
		self.fileList = []
		self.df = None
		self.dateObj = SDateTime()
		self.plot = SPlot(_fdir)
		self.__checkDir(self.dir)
		_tDir = join(self.dir, 'tmp')
		self.__checkDir(_tDir)
		self.tJSON = join(_tDir,'temp.json')

	#def fileChooser(self):
	#	root = tk.Tk()
	#	root.withdraw()
	#	file_path = filedialog.askopenfilenames(initialdir = self.dir, title = "Select file", filetypes = (("JSON files","*.json"),("all files","*.*")))
	#	self.fileList = [i for i in root.tk.splitlist(file_path)]

	def __checkDir(self, _dir):
		if not os.path.exists(_dir):
			try:
			    os.makedirs(_dir)
			except OSError as e:
			    if e.errno != errno.EEXIST:
			        raise

	def getLogFiles(self, runtime="", locality=-1, startDate=None, endDate=None):
		_fileList = [f for f in listdir(self.dir) if isfile(join(self.dir, f))]
		self.fileList = []

		if len(runtime) >0 or locality>=0 or (startDate is not None and endDate is None):
			# filter based on criteria
			fs = ''
			if len(runtime)>0:
				fs = runtime.upper() + "_"
		
			if locality >=0:
				fs = fs + str(locality) + "_"

			if startDate is not None and endDate is None:
				fs = fs + str(startDate)

			#print(fs)

			[self.fileList.append(join(self.dir, f)) for f in _fileList if fs in f]

			#print(self.fileList)

		elif startDate is not None and endDate is not None:
			date_range = [startDate + dt.timedelta(days=x) for x in range(0, (endDate-startDate).days)]
			
			fs = ''
			if len(runtime)>0:
				fs = runtime.upper() + "_"
		
			if locality >=0:
				fs = fs + str(locality) + "_"

			for _date in date_range:
				[self.fileList.append(join(self.dir, f)) for f in _fileList if (fs + str(_date)) in f]

	def __printFiles(self):
		print(self.fileList)

	def readFiles(self):
		if len(self.fileList) > 0:
			print("Starts processing JSON object...")
			if isfile(self.tJSON):
				os.remove(self.tJSON)

			with open(self.tJSON, 'w') as outfile:
				outfile.write("[")
				input_lines = fileinput.input(files=self.fileList)
				a = 0
				for line in input_lines:
					a += 1
				input_lines = fileinput.input(files=self.fileList)
				b=0
				for line in input_lines:
					b +=1

					if (line[-1:]!=','):
						line = line[:-1]
					if b==a:
						line = line[:-1]+"]"
					
					outfile.write(line)

			print("Converting JSON to dataframe...")
			self.df = pd.DataFrame(pd.read_json(self.tJSON, "record"))
			print("Conversion completed...")


	def eventFrequencyPlot(self, xlab="", ylab="", xRotation = 0, yRotation = 0, fullTitle="", fontSizeX=7, fontSizeY=7, xScale="", yScale=""):
		if self.df is None:
			self.readFiles();

		if self.df is not None:
			df_ENC = self.df.groupby('EN').size().reset_index(name='Counts')
			# plotting histogram
			self.plot.barPlot(df_ENC['EN'], df_ENC['Counts'], xlabel=xlab, ylabel=ylab, xRot = xRotation, yRot = yRotation, title=fullTitle, xFontSize=fontSizeX, yFontSize=fontSizeY, scaleX=xScale, scaleY=yScale)

	def eventMatricPlot(self, matricColName='ET', matricName='Execution time', matricUnit="s", xlab="", ylab="", xRotation = 0, yRotation = 0, fullTitle="", fontSizeX=7, fontSizeY=7, xScale="", yScale=""):
		if self.df is None:
			self.readFiles();

		cn = matricName
		if len(cn)==0:
			cn = 'abc'

		if self.df is not None:
			df_ENT = self.df.groupby('EN')[matricColName].sum().reset_index(name=cn)
			# plotting histogram
			self.plot.barPlot(df_ENT['EN'], df_ENT[cn], xlabel=xlab, ylabel=ylab, xRot = xRotation, yRot=yRotation, title=fullTitle, xFontSize=fontSizeX, yFontSize=fontSizeY, scaleX=xScale, scaleY=yScale)

	def __timeDistanceofAnEvent(self, st=None, unit="s"):
		if (st is None):
			ats = sorted(self.df['TS'])
			st = self.dateObj.formatDateTime(ats[0])

		t = self.dateObj.formatDateTime(st)
		self.df['TIS'] = [(self.dateObj.formatDateTime(i)-t).total_seconds() for i in self.df['TS']]

		unit = unit.lower()
		# minutes
		if(unit == "m" or unit == "min"): 
			self.df['TIS'] = [i/60 for i in self.df['TIS']]
		# hours
		elif(unit == "h" or unit == "hr" or unit == "hour"):
			self.df['TIS'] = [i/(60*60) for i in self.df['TIS']]
		# day
		elif(unit == "d" or unit == "day"):
			self.df['TIS'] = [i/(60*60*24) for i in self.df['TIS']]
		# month (assuming each month is 30 days long)
		elif(unit == "mn" or unit == "month"):
			self.df['TIS'] = [i/(60*60*24*30) for i in self.df['TIS']]


	'''
	Location string     Location code	boxToAnchor (to place the legend outside the plotting area)
	===============     =============	============
	'best'					0
	'upper right'			1
	'upper left'			2
	'lower left'			3 			(1.04,0)
	'lower right'			4 			(1,0)
	'right'					5
	'center left'			6 			(1.04,0.5)
	'center right'			7
	'lower center'			8
	'upper center'			9
	'center'				10
	'''
	def analyzeEventMatric(self, en="", matr="", ts=10, unit="s", _xlabel="", _ylabel="", _title="", _xRot=0, _yRot=0, _xFontSize=10, _yFontSize=10, _save=True, _fileName="", 
				_scaleX="", _scaleY="", showXMajorTics=True, showXMinorTics=False, xMajorTics=10, xMinorTics=5, showYMajorTics=True, 
				showYMinorTics=False, yMajorTics=10, yMinorTics=5, alphaMajor=0.5, alphaMinor=0.2, _color='k', _lineType='-', 
				_lineWidth=2, _fdpi=350, _showGrid=False, multiLine=False):

		if self.df is None:
			self.readFiles();

		if len(matr)==0:
			matr = ['ET', 'IS', 'OS', 'LI']
		else:
			matr = [matr]

		if self.df is not None:
			if len(en)==0:
				en = [i for i in self.df.EN.unique()]
			else:
				en = [en]

			ats = sorted(self.df['TS'])
			st = self.dateObj.formatDateTime(ats[0])
			et = self.dateObj.formatDateTime(ats[len(ats)-1])
			
			dur = (et-st).total_seconds()

			unit = unit.lower()
			tunit = "seconds"
			# minutes
			if(unit == "m" or unit == "min"): 
				dur = dur/60
				tunit = "minutes"
			# hours
			elif(unit == "h" or unit == "hr" or unit == "hour"):
				dur = dur/(60*60)
				tunit = "hours"
			# day
			elif(unit == "d" or unit == "day"):
				dur = dur/(60*60*24)
				tunit = "days"
			# month (assuming each month is 30 days long)
			elif(unit == "mn" or unit == "month"):
				dur = dur/(60*60*24*30)
				tunit = "months"

			if(dur%ts!=0):
				dur = dur + ts

			tl = list(np.arange(0, dur, ts))
			
			self.__timeDistanceofAnEvent(ats[0], unit)

			cv = [i for i in range(1, len(en)+1, 1)]
			cmap = matplotlib.cm.viridis
			norm = matplotlib.colors.Normalize(vmin=min(cv), vmax=max(cv))

			_xRange = [min(self.df['TIS']), max(self.df['TIS'])]

			# For each matric (execution time/input size/output size/loop counter)
			for _tmn in matr:
				_yRange = [min(self.df[_tmn]), max(self.df[_tmn])]
				yMajorTics = (max(self.df[_tmn]) - min(self.df[_tmn]))/ts
				
				j=0
				col = []
				x=[]
				y=[]

				mn = ''
				yl = ''
				_title = ''
				if _tmn == "ET":
					mn = "Execution time"
					yl = "Execution time (" + tunit + ")"
					_title = "Event execution time plot"
				elif _tmn == "IS":
					mn = "Input size"
					yl = "Input size (byte)"
					_title = "Event input size plot"
				elif _tmn == "OS":
					mn = "Output size"
					yl = "Output size (byte)"
					_title = "Event output size plot"
				elif _tmn == "LI":
					mn = "Loop counter"
					yl = "Loop counter"
					_title = "Event loop counter plot"

				for _ten in en:
					if len(en)>1:
						col.append(cmap(norm(cv[j])))
						j = j + 1
					else:
						col.append(_color)

					tdf = self.df[self.df['EN'] == _ten]

					if multiLine==True:
						x.append(tdf['TIS'])
						y.append(tdf[_tmn])
						_fileName = yl + " for all events and " + mn + "_" + str(dt.datetime.now())
					else:
						_fileName = yl + " for event (" + _ten + ")_" + str(dt.datetime.now())

					if multiLine==False:
						_yRange = [min(tdf[_tmn]), max(tdf[_tmn])]
						yMajorTics = (max(tdf[_tmn]) - min(tdf[_tmn]))/ts

						self.plot.lineChartPlot([tdf['TIS']], [tdf[_tmn]], xRange=_xRange, yRange=_yRange, xlabel=_xlabel, ylabel=yl, title=_title + " for " + _ten, xRot=_xRot, yRot=_yRot, xFontSize=_xFontSize, yFontSize=_yFontSize, save=_save, fileName=_fileName, 
							scaleX=_scaleX, scaleY=_scaleY, _showXMajorTics=showXMajorTics, _showXMinorTics=showXMinorTics, _xMajorTics=xMajorTics, _xMinorTics=xMinorTics, _showYMajorTics=showYMajorTics, 
							_showYMinorTics=showYMinorTics, _yMajorTics=yMajorTics, _yMinorTics=yMinorTics, _alphaMajor=alphaMajor, _alphaMinor=alphaMinor, color=col, lineType=_lineType, 
							lineWidth=_lineWidth, fdpi=_fdpi, showGrid=_showGrid, showLegends=False)

			
				if multiLine==True:
					self.plot.lineChartPlot(x, y, xRange=_xRange, yRange=_yRange, xlabel=_xlabel, ylabel=yl, title=_title, xRot=_xRot, yRot=_yRot, xFontSize=_xFontSize, yFontSize=_yFontSize, save=_save, fileName=_fileName, 
						scaleX=_scaleX, scaleY=_scaleY, _showXMajorTics=showXMajorTics, _showXMinorTics=showXMinorTics, _xMajorTics=xMajorTics, _xMinorTics=xMinorTics, _showYMajorTics=showYMajorTics, 
						_showYMinorTics=showYMinorTics, _yMajorTics=yMajorTics, _yMinorTics=yMinorTics, _alphaMajor=alphaMajor, _alphaMinor=alphaMinor, color=col, lineType=_lineType, 
						lineWidth=_lineWidth, fdpi=_fdpi, showGrid=_showGrid, showLegends=True, legends=en, legendPos='center left')


# Class handles user request from command prompt
class SLogMenu:
	def __init__(self, argv):
		self.ldir = ''
		self.fdir = ''
		self.runtime = ''
		self.locality=-1
		self.date=None
		self.startDT = None
		self.endDT = None
		self.eventCount = False
		self.eventCountScaleY = ''
		self.eventMatricCount = False
		self.eventMatricCountScaleY = ''
		self.eventMatricName = 'ET'
		self.EventMatricUnit = "s"
		self.eventMatric = False
		self.eventMatricScaleY=''
		self.eventName = ''
		self.matricName = ''
		self.timeInterval = 10
		self.timeUnit = 's'
		self.multiLine = False
		
		self.dateObj = SDateTime()
		self.__parseArgs(argv)

	def __helpMessage(self):
		print('-h or --help: help message' + 
	      		'\n-d: Directory of logging file [Required field]' + 
	      		'\n-f: Directory of saved image [If not provided then it will use the location provided in -d. Directory must have write permission.]' +
	      		'\n-r: Runtime [Optional]' + 
	      		'\n-l:Locality [Optional]' + 
	      		'\n--dt: Date in a format of mm-dd-yyyy [Optional]' + 
	      		'\n--dtr: Date range. Eg. 07-12-2016T09-03-2017 [Optional]' + 
	      		'\n--ec: Plot the number of times an event is called' + 
	      		'\n--ecy: Scale y axis values [optional, default value ''], eg. log' + 
	      		'\n--emc: Plot aggregate matric count vs event. You have to pass the matric name using --emn parameter.' + 
	      		'\n--emcy: Scale y axis values [optional, default value ''], eg. log' + 
	      		'\n--emn: Event matric name, eg. execution time (ET), input size (IS), output size (OS), loop counter (LI)' + 
	      		'\n--emu: Event matric unit: for execution time: Milisecond (ms), Minute (m or min), Hour (h or hr or hour), Day (d or day), Month (mn or month), and default unit is second; For IS or OS: kilo (kb), mega (mb), giga (gb), tera (tb), default is byte (b)' + 
	      		'\n--em: Flag to enable functionality to plot event vs matric [By default it will plot event vs matric for ]' + 
	      		'\n--en: Event name' + 
	      		'\n--emy: Scale y axis values [optional, default value ''], eg. log' + 
	      		'\n--mn: Matric name, eg. execution time (ET), input size (IS), output size (OS), loop counter (LI)' + 
	      		'\n--ti: Time interval in the time line' + 
	      		'\n--tu: Time interval unit, eg. Milisecond (ms), Minute (m or min), Hour (h or hr or hour), Day (d or day), Month (mn or month), and default unit is second (s)' + 
	      		'\n--ml: Show all event time lines in a single chart [optional, default: False]' +
	      		'\n')
	
	def __parseDateRange(self, dt):
		dtl = dt.split("T")
		self.startDT = self.dateObj.formatDateTime(dtl[0], "%m-%d-%Y").date()
		self.endDT = self.dateObj.formatDateTime(dtl[1], "%m-%d-%Y").date()


	def __parseArgs(self, argv):
		try:
			opts, args = getopt.getopt(argv, 'hd:f:r:l:', ["help","dt=","dtr=","ec","ecy=","emc","emcy=","emn=","emu=","em","emy=","en=","mn=","ti=","tu=","ml"])
		except getopt.GetoptError as err:
			print(err)
			self.__helpMessage()
			sys.exit(2)

		for opt, arg in opts:
			if opt in ("-h", "--help"):
				helpMessage()
				sys.exit()
			elif opt == "-d":
				self.ldir = arg
			elif opt == "-f":
				self.fdir = arg
			elif opt == "-r":
				self.runtime = arg
			elif opt == "-l":
				self.locality = int(arg)
			elif opt == "--dt":
				self.date = self.dateObj.formatDateTime(arg, format="%m-%d-%Y").date()
			elif opt == "--dtr":
				self.__parseDateRange(arg)
			elif opt == "--ec":
				self.eventCount = True
			elif opt == "--ecy":
				self.eventCountScaleY = arg
			elif opt == "--emc":
				self.eventMatricCount = True
			elif opt == "--emcy":
				self.eventMatricCountScaleY = arg
			elif opt == "--emn":
				self.eventMatricName = arg
			elif opt == "--emu":
				self.EventMatricUnit = arg
			elif opt == "--em":
				self.eventMatric = True
			elif opt == "--emy":
				self.eventMatricScaleY = arg
			elif opt == "--en":
				self.eventName = arg
			elif opt == "--mn":
				self.matricName = arg
			elif opt == "--ti":
				self.timeInterval = float(arg)
			elif opt == "--tu":
				self.timeUnit = arg
			elif opt == "--ml":
				self.multiLine = True

		if len(self.ldir)==0:
			self.__helpMessage()
			sys.exit()

		if len(self.fdir)==0:
			self.fdir = self.ldir

	def __test(self):
		print("log dir=",self.ldir)
		print("img dir=", self.fdir)
		print("runtime=",self.runtime)
		print("locality=",self.locality)
		print("Date=",self.date)
		print("Start date=", self.startDT)
		print("End date=", self.endDT)
		print("Event count=", self.eventCount)
		print("Event count scale Y=", self.eventCountScaleY)
		print("Event Matric count=", self.eventMatricCount)
		print("Event Matric count scale Y=", self.eventMatricCountScaleY)
		print("Event Matric Name=", self.eventMatricName)
		print("Event matric=", self.eventMatric)
		print("Event matric Scale Y=", self.eventMatricScaleY)
		print("Event name=", self.eventName)
		print("Matric name=", self.matricName)
		print("Time interval=", self.timeInterval)
		print("Time unit=", self.timeUnit)

	def generateReport(self):
		self.__test()
		logObj = SLogAnalysis(self.ldir, self.fdir)

		_date = None
		if self.startDT is not None:
			_date = self.startDT
		elif self.date is not None:
			_date = self.date

		logObj.getLogFiles(runtime=self.runtime, locality=self.locality, startDate=_date, endDate=self.endDT)
		
		if self.eventCount==True:
			logObj.eventFrequencyPlot(xlab = "Event name", ylab="Frequency", xRotation=30, fullTitle="Event frequency plot", yScale=self.eventCountScaleY)
		
		if self.eventMatricCount == True:
			mn = ''
			yl = ''
			title = ''
			if self.eventMatricName == "ET":
				mn = "Execution time"
				yl = "Aggregated execution time (sec)"
				title = "Event execution time plot"
			elif self.eventMatricName == "IS":
				mn = "Input size"
				yl = "Aggregated input size (byte)"
				title = "Event input size plot"
			elif self.eventMatricName == "OS":
				mn = "Output size"
				yl = "Aggregated output size (byte)"
				title = "Event output size plot"
			elif self.eventMatricName == "LI":
				mn = "Loop counter"
				yl = "Aggregated loop counter"
				title = "Event loop counter plot"

			if self.eventMatricCountScaleY.upper() == "LOG":
				yl += " in logarithmic scale"

			logObj.eventMatricPlot(matricColName=self.eventMatricName, matricName=mn, xlab = "Event name", ylab=yl, xRotation=30, fullTitle=title, yScale=self.eventMatricCountScaleY)
		
		if self.eventMatric==True:
			
			tunit = 'seconds'
			# miliseconds
			if(self.timeUnit.upper() == "MS"): 
				tunit = "miliseconds"
			# minutes
			if(self.timeUnit.upper() == "M" or self.timeUnit.upper() == "MIN"): 
				tunit = "minutes"
			# hours
			elif(self.timeUnit.upper() == "H" or self.timeUnit.upper() == "HR" or self.timeUnit.upper() == "HOUR"):
				tunit = "hours"
			# day
			elif(self.timeUnit.upper() == "D" or self.timeUnit.upper() == "DAY"):
				tunit = "days"
			# month (assuming each month is 30 days long)
			elif(self.timeUnit.upper() == "MN" or self.timeUnit.upper() == "MONTH"):
				tunit = "months"

			logObj.analyzeEventMatric(en=self.eventName, matr=self.matricName, ts=self.timeInterval, unit=self.timeUnit, 
				_xlabel="Time in " + tunit , _xFontSize=7, _yFontSize=7, _scaleY=self.eventMatricScaleY, 
				_showGrid=True, xMajorTics=self.timeInterval, multiLine=self.multiLine)

# Main function
if __name__ == "__main__":
	menu = SLogMenu(sys.argv[1:])
	menu.generateReport()

# test command:
# python3 ./slog.py -d "/Users/methun/self/PNNL/Work/Shad_log/log_2018-08-15" -f "/Users/methun/self/PNNL/Work/Shad_log/logfig" --dt "08-15-2018" --ec --em --ti 5 --ml

