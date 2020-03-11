from timeit import default_timer as timer


def elapsed_time(func):
    def wrapper(*args, **kwargs):
        init = timer()
        func(*args, **kwargs)
        end = timer()

        return (end - init)
    return wrapper
