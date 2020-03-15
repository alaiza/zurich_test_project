import sys
import argparse
from test.alaiza_project.MAIN import main_zurich
from test.libs.logger import specific_logger

sys.path.insert(0, 'src.zip')





def build_argument_parser():
    parser = argparse.ArgumentParser(description='Test_berlin_project_parser')
    parser.add_argument("--costtype", required=False, type=str, default='1',choices=['fixed','startbased'], help="type of cost calculation desired")
    parser.add_argument("--tocsv", required=False, type=str, default='yes',choices=['yes','no'], help="this option will export to a cvs file with the solution")
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

