#! /usr/bin/env python
""""
-----------------------------------------------------------------------------------------
File for handling logging.
-----------------------------------------------------------------------------------------
"""

import logging
from datetime import datetime
from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET # log levels

DEFAULT_LOG_FORMAT = '[%(asctime)s %(levelname)8s] %(message)s'
TRACEABLE_LOG_FORMAT = (
	'[%(asctime)s %(levelname)8s in %(name)s:%(lineno)d] %(message)s')
EXTRA_TRACEABLE_LOG_FORMAT = (
	'[%(asctime)s %(levelname)8s in %(filename)s:%(lineno)d - %(funcName)s] %(message)s')

FORMAT_BY_LEVEL = {
	DEBUG: TRACEABLE_LOG_FORMAT,
	WARNING: TRACEABLE_LOG_FORMAT,
	ERROR: TRACEABLE_LOG_FORMAT,
	CRITICAL: TRACEABLE_LOG_FORMAT,
}

class LevelFormatter(logging.Formatter):

	def __init__(self, format_by_level={}, default_fmt='%(message)s', datefmt=None):
		self._level_formatters = {}
		for level, frmt in format_by_level.items():
			self._level_formatters[level] = logging.Formatter(
				fmt=frmt, datefmt=datefmt
			)

		super(LevelFormatter, self).__init__(fmt=default_fmt, datefmt=datefmt)


	def format(self, record):
		if record.levelno in self._level_formatters:
			return self._level_formatters[record.levelno].format(record)
		
		return super(LevelFormatter, self).format(record)


def get_logger(
	name, 
	level=INFO, 
	format_by_level=FORMAT_BY_LEVEL, 
	default_log_format=DEFAULT_LOG_FORMAT,
	datefmt='%Y-%m-%d %H:%M:%S%z'
):
	"""
	Gets logger in singleton pattern so as to avoid duplicating logs across 
	multiple processes.
	"""

	logger = logging.getLogger(name)

	if len(logger.handlers) < 1:
# 		logger.addHandler(logging.FileHandler('{}_{}.log'.format(name, datetime.now().isoformat())))
		logger.addHandler(logging.StreamHandler())

	logger.setLevel(level)
	formatter = LevelFormatter(
		format_by_level, 
		default_log_format,
		datefmt=datefmt
	)
	for handler in logger.handlers:
		handler.setLevel(level)
		handler.setFormatter(formatter)

	return logger


def set_verbosity(verbosity, *logs):
	"""
	Set verbosity of logs according to following dictionary.
	0: CRITICAL/FATAL,
	1: ERROR,
	2: WARN/WARNING,
	3: INFO,
	4: DEBUG
	
	Or accept one of CRITICAL, ERROR, WARNING, INFO, or DEBUG directly.
	"""
	if verbosity not in {CRITICAL, ERROR, WARNING, INFO, DEBUG}:
		verbosity = max(CRITICAL - verbosity * (CRITICAL - ERROR), DEBUG)
	for log in logs:
		log.setLevel(verbosity)

