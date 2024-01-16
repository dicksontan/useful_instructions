import logging
import os
from functools import wraps
from time import perf_counter, sleep
import datetime as dt
from typing import Callable

def log_it(*args):

    '''

    Example usage:  log1 = log_it('a','_b')
    log1.info(f'{ctry} succed')

    '''

    if not os.path.isdir("./logs"):
        os.makedirs("./logs")

    str1 = ''

    for x in args:
        str1+=x

    logging.basicConfig(level=logging.INFO)

    log1 = logging.getLogger(str1)
    #log1.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s -> %(message)s"
    )
    
    file_handler = logging.FileHandler("logs/" + f"{str1}.log")
    file_handler.setFormatter(formatter)
    log1.addHandler(file_handler)

    return log1

def log_wrapper_outer(folder_name):

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    def log_wrapper_inner(func):

        @wraps(func)
        def wrapper(*args,**kwargs):

            logging.basicConfig(filename=f'{folder_name}/{func.__name__}_log.txt',level=logging.INFO)

            # Create a logger object
            #logger = logging.getLogger(func.__name__)
            
            # Add a file handler to the logger
            #file_handler = logging.FileHandler(f'{folder_name}/{func.__name__}_log.txt')
            #formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            #file_handler.setFormatter(formatter)
            #logger.addHandler(file_handler)

            begin_func_time = dt.datetime.now()

            start_time = perf_counter()

            logging.info(f'{func.__name__} ran at {begin_func_time} with inputs {args}, {kwargs}')

            try:

                func(*args,**kwargs)

                end_time = perf_counter()

                total_time = end_time - start_time

                logging.info(f'{func.__name__}, started: {begin_func_time}, inputs: {args}, {kwargs}, total_run_time: {total_time}, ended: {dt.datetime.now()}')

            except Exception as e:

                logging.error(f'Error:{e}')

        return wrapper

    return log_wrapper_inner

    #sample of how to use it
    #from zdt.log_func import log_wrapper_outer
    # @log_wrapper_outer('logs')
    # def func2(x,y,*args,**kwargs):
    #     print('x:', x)
    #     print('y+1:', y+1)
    #     print(args)
    #     print(kwargs)

    # func2('x1',1,3,0,c=2,d=3)