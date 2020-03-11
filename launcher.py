from src.main import main
import importlib




if __name__ == "__main__":
    try:
        #module = importlib.import_module('main')
        main()
    except Exception, ex:
        print ex
