#-------------------------------------------------------------------------------
# System Imports
#-------------------------------------------------------------------------------

import json
import os

#-------------------------------------------------------------------------------
# Local Imports
#-------------------------------------------------------------------------------

from ..utils import logger

#-------------------------------------------------------------------------------
# Globals
#-------------------------------------------------------------------------------

logr = logger.get_logger(__name__, level="DEBUG")

################################################################################
# Read the configuration file...
#
def read(
    fname,
    logr,
    validate=True,
):
    """
    """
    if not os.path.exists(fname):
        logr.error("JSON config '{}' does not exist".forma(fname))
        return None
    
    # Parse the config file..
    tickers_json = None
    with open(fname) as fp:
        tickers_json = json.load(fp)

    # Determine if the configuration file is valid
    if validate:
        logr.debug("Validating config...")
        if validate(tickers_json):
            logr.debug("PASSED")
        else:
            logr.debug("FAILDED")
            tickers_json = None

    return tickers_json

################################################################################
# Validate configuration file...
#
def validate(config_json):
    """
    """
    pass