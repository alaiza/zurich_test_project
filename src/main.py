import sys
import argparse
import os
from test.alaiza_project.MAIN import main_zurich
from test.libs.logger import specific_logger

sys.path.insert(0, 'src.zip')





def build_argument_parser():
    parser = argparse.ArgumentParser(description='Test_berlin_project_parser')
    parser.add_argument("--reptime", required=False, type=int, default='1', help="minutes of difference between API calls (default 1)")
    parser.add_argument("--exectime", required=False, type=int, default='99999999', help="execution time in minutes (default 60)")
    return parser



def main():
    try:
        logger = specific_logger()
        parser = build_argument_parser()
        arguments = vars(parser.parse_args())
        main_zurich(arguments, logger)

    except Exception, ex:
        print ex


if __name__ == "__main__":
    main()

