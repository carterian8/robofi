#-------------------------------------------------------------------------------
# System Imports
#-------------------------------------------------------------------------------

import argparse
import sys

#-------------------------------------------------------------------------------
# Local Imports
#-------------------------------------------------------------------------------

from ..pricepredict import traintest

################################################################################
################################################################################
# Gets the command line arguments...
#
def parse_cmd_line(as_dict=False):
	"""
	Description:
		Parses the command line arguments for the program
	Parameters:
		as_dict - bool
			Returns the args as a dict. Default=False
	Returns:
		The command line arguments as a dictionary or a Namespace object and the
		parser used to parse the command line.
	"""

	defaults = {
		"log_level" : "INFO"
	}
	
	parser = argparse.ArgumentParser(
		description="""robofi - Automated financial portfolio management""",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter
	)
	parser.set_defaults(default_fn=lambda: print("foo"))
	subparsers = parser.add_subparsers(title="subcommands")
	
	# scraper subparser...
	
	parser_scraper = subparsers.add_parser("pricepredict")
	parser_scraper.add_argument("json_config",
		help="""Main configuration file"""
	)
	parser_scraper.add_argument("--log_level", 
		choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
		default=defaults["log_level"],
		help="""Log level of the program."""
	)
	parser_scraper.set_defaults(func=traintest.run)
	
	args = parser.parse_args()
	if as_dict:
		args = vars(args)

	return args, parser

################################################################################
################################################################################
# Main...
#
def main():
	
	# Get the command line arguments...

	args, parser = parse_cmd_line(as_dict=True)

	# If no sub command was provided, print the usage text...
	
	if not "func" in args:
		parser.print_help()
		sys.exit()

	# Execute the main fuction for the subcommand chosen...

	args["func"](args)
	

################################################################################
################################################################################
# Entry point...
#
if __name__ == "__main__":
	main()
    