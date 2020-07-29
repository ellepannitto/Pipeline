import functools
import logging
from multiprocessing.pool import AsyncResult
import tqdm
import multiprocessing as mp
from multiprocessing import TimeoutError

from typing import List

logger = logging.getLogger(__name__)


def grouper (iterable, n):
    out = []
    for x in iterable:
        out.append (x)
        if len(out) == n:
            yield out
            out = []
    if out:
        yield out

def wrap_function_result_inside_a_list ( function, input ):
    return [function(input)]

class Farm:

    def __init__(self, list_of_functions, nworkers, batchsize=1, show_progress_bar=True, timeout=0.001):
        self.functions = list_of_functions
        self.nworkers = nworkers
        self.show_progress_bar = show_progress_bar
        self.batchsize = batchsize
        self.timeout = timeout

        assert batchsize>0, "batchsize must be greater than zero"
        assert nworkers>0, "nworkers must be greater than zero"

        
    def parallel_process(self, x):

        for func in self.functions:
            x = sum(func(x), [])
        return x

    def map(self, iterable_input):

        iterable = grouper(tqdm.tqdm(iterable_input, desc="farm input", disable=not self.show_progress_bar), self.batchsize)
        
        if self.nworkers == 1:
            for y in map (self.parallel_process, iterable):
                yield y
        else:

            results_handlers: List[AsyncResult] = []
            with mp.Pool (self.nworkers) as pool:

                # get nworkers batches from the input iterable, submit them to the pool 
                for _,task in zip(range (self.nworkers), iterable):
                    handle = pool.apply_async ( self.parallel_process, [task] )
                    results_handlers.append (handle)
                
                # iterate the remaining batches, get one batch at a time and submit it when a previous task is completed
                for task in iterable:
                    i=0
                    do_next_task = False
                    while not do_next_task:
                        handle = results_handlers[i]
                        try:
                            y = handle.get ( self.timeout )
                            del results_handlers[i]
                            h = pool.apply_async (self.parallel_process, [task])
                            results_handlers.append (h)
                            yield y
                            do_next_task = True

                        except TimeoutError:
                            i = (i+1) % len(results_handlers)

                i = 0
                # wait until all the tasks are completed
                while len(results_handlers):
                    handle = results_handlers[i]
                    try:
                        y = handle.get ( self.timeout )
                        del results_handlers[i]
                        yield y
                        i = 0

                    except TimeoutError:
                        i = (i+1) % len(results_handlers)

    def map_reduce (self, iterable_input, reduce_fn, reduce_batch):

        iterable = grouper(tqdm.tqdm(iterable_input, desc="farm input", disable=not self.show_progress_bar), self.batchsize)
        
        if self.nworkers == 1:
            first = True
            out = None
            for y in grouper (map (self.parallel_process, iterable), reduce_batch):
                y = sum (y, [])
                out = reduce_fn ( y + ([] if first else [out]) )
                first = False
            return out
        else:

            partial_reduce = functools.partial (wrap_function_result_inside_a_list, reduce_fn)
            cached_results = []
            results_handlers: List[AsyncResult] = []
            with mp.Pool (self.nworkers) as pool:

                # get nworkers batches from the input iterable, submit them to the pool 
                for _,task in zip(range (self.nworkers), iterable):
                    handle = pool.apply_async ( self.parallel_process, [task] )
                    results_handlers.append (handle)
                
                # iterate the remaining batches, get one batch at a time and submit it when a previous task is completed
                for task in iterable:
                    i=0
                    do_next_task = False
                    while not do_next_task:
                        handle = results_handlers[i]
                        try:
                            y = handle.get ( self.timeout )
                            del results_handlers[i]
                            h = pool.apply_async (self.parallel_process, [task])
                            results_handlers.append (h)
                            cached_results.extend (y)
                            do_next_task = True

                        except TimeoutError:
                            i = (i+1) % len(results_handlers)
                    
                    if len(cached_results) >= reduce_batch:
                        h = pool.apply_async (partial_reduce, [cached_results])
                        results_handlers.append (h)
                        cached_results = []                      

                i = 0
                # wait until all the tasks are completed
                while len(results_handlers):
                    handle = results_handlers[i]
                    try:
                        y = handle.get ( self.timeout )
                        del results_handlers[i]
                        cached_results.extend (y)
                        i = 0

                    except TimeoutError:
                        i = (i+1) % len(results_handlers)
                
                return reduce_fn (cached_results)



