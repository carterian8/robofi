#-------------------------------------------------------------------------------
# System Imports
#-------------------------------------------------------------------------------

import sys

#-------------------------------------------------------------------------------
# Local Imports
#-------------------------------------------------------------------------------

import configs
import datasets
import models
from ..utils import logger

#-------------------------------------------------------------------------------
# Globals
#-------------------------------------------------------------------------------

logr = logger.get_logger(__name__, level="DEBUG")

################################################################################
# ...
#
def run(cl_args):
    """
    """
    logr.setLevel(cl_args["log_level"])
    logr.info("Running pricepredict")

    # Read the configuration file, exit if theres an error
    logr.info("Reading configuration file '{}'".format(cl_args["json_config"]))
    config = configs.read(cl_args["json_config"], logr)
    if config is None:
        logr.error("Error reading config file, aborting")
        sys.exit(-1)

################################################################################
# ...
#
def train(cl_args):
    """
    """
    pass

################################################################################
# ...
#
def test(cl_args):
    """
    """
    pass