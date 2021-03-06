import functools
import logging
from multiprocessing.pool import AsyncResult
import tqdm
import multiprocessing as mp
from multiprocessing import TimeoutError
import os

from typing import List
from collections import namedtuple

TaskResult = namedtuple("TaskResult", ["data", "level"])

logger = logging.getLogger(__name__)


def grouper(iterable, n):
    out = []
    for x in iterable:
        out.append(x)
        if len(out) == n:
            yield out
            out = []
    if out:
        yield out


def wrap_reduce_function(function, cur_input, result_level=0) -> TaskResult:
    logger.debug("[PROCESS-{}]  started reduce".format(os.getpid()))
    y = function(cur_input)
    logger.debug("[PROCESS-{}] finished reduce".format(os.getpid()))
    return TaskResult([y], result_level)


class Farm:
    '''
     Helper class to compute composition of functions over an input iterable,
     using multiprocessing for heavy computations.
     Implements the map and map-reduce paradigms. 

     `function composition`
     The composition of a list of functions f_1, f_2, ... f_m is a function g
     defined as g(x) := f_1 (f_2 ( ... f_m (x) ) )
     
     `map paradigm`
     An iterable input X = {x_1, x_2 ... x_n} can be mapped using a function g resulting
     into an iterable Y := {g(x_1), g(x_2), ..., g(x_n)}
     This works just like the map builtin function: https://docs.python.org/3/library/functions.html#map

     `reduce paradigm`
     An iterable Y = {y_1, y_2, ..., y_n} can be reduced using a function h resulting
     in h( h( h( ..., h( h(y_1, y_2), y_3 ), ..., y_n )))
     This works just like functools.reduce https://docs.python.org/3/library/functools.html#functools.reduce
     if h is associative and commutative it can be applied in any order to the elements y_i

     `map-reduce paradigm`
     the map-reduce paradigm simply consist into mapping an input iterable using a function g and
     then reducing the result using another function h.

     This class implements the map-reduce paradigm with some variants:
      1. the function g used for the map phase is the composition of a list of functions f_1, f_2, ... f_m
      2. each function f_i is actually a python generator that accepts a list of inputs [x_1, ... x_b] and
         yields zero or more lists of results to be processed by f_i+1
      3. the reduction function h must be commutative and associative.
      4. h accepts a list of inputs [y_1, ... y_r] and returns the reducted result.
         In this way only approx. r partial results are kept in memory at any given moment rather
         than the whole iterable Y.

    b (the batch size for the map phase) and r (the batch size for the reduction phase) are parameters
    that must be fine-tuned in order to improve the performances when using multiprocessing
    '''

    def __init__(self, list_of_functions, nworkers, batchsize=1, show_progress_bar=True):
        '''
         initializes a Farm object.

         :param: list_of_functions generators f_1, ... f_n to be composed.
         See the documentation for the class Farm for details.
         :param: nworkers maximum number of processes to be spawn for both the map and the reduce phase.
         :param: batchsize number of input elements to be given in input to f_1.
         The input is chunked only when nworkers>1
         :param: show_progress_bar whether to show a progress bar to monitor the number of input elements
         processed by map() and map_reduce()

        '''

        self.functions = list_of_functions
        self.nworkers = nworkers
        self.show_progress_bar = show_progress_bar
        self.batchsize = batchsize
        self.timeout = 0.001

        assert batchsize > 0, "batchsize must be greater than zero"
        assert nworkers > 0, "nworkers must be greater than zero"

    def parallel_process(self, x) -> TaskResult:

        logger.debug("[PROCESS-{}]  started parallel_process".format(os.getpid()))

        for i, func in enumerate(self.functions):
            logger.debug("[PROCESS-{}]  calling stage {} function ({} input items)".format(os.getpid(), i, len(x)))
            x = sum(func(x), [])

        logger.debug("[PROCESS-{}] finished parallel_process".format(os.getpid()))

        return TaskResult(x, 0)

    def get_result_and_delete_handler(self, results_handlers) -> TaskResult:
        i = 0
        is_result_available = False
        y = None
        while not is_result_available:
            handle = results_handlers[i]
            try:
                y = handle.get(self.timeout)
                del results_handlers[i]
                is_result_available = True

            except TimeoutError:
                i = (i+1) % len(results_handlers)
        return y

    def map(self, iterable_input):
        '''
         applies the functions composition to each element of an input iterable.
         See the documentation for the class Farm for details.

         Yelds g(x_1), g(x_2) ... g(x_n) where g is the composition of the functions specified in
         the constructor and X is the input iterable.
         The results are not yielded in order when nworkers > 1

         :param: iterable_input the iterable to map
        '''

        iterable = grouper(tqdm.tqdm(iterable_input, desc="farm input", disable=not self.show_progress_bar),
                           self.batchsize)

        if self.nworkers == 1:
            for y in map(self.parallel_process, iterable):
                yield y.data
        else:
            results_handlers: List[AsyncResult] = []
            with mp.Pool(self.nworkers) as pool:

                # get nworkers batches from the input iterable, submit them to the pool 
                for _, task in zip(range(self.nworkers), iterable):
                    handle = pool.apply_async(self.parallel_process, [task])
                    results_handlers.append(handle)
                
                # iterate the remaining batches, get one batch at a time and submit it when a previous task is completed
                for task in iterable:
                    y = self.get_result_and_delete_handler(results_handlers)
                    h = pool.apply_async(self.parallel_process, [task])
                    results_handlers.append(h)
                    yield y.data

                while len(results_handlers):
                    y = self.get_result_and_delete_handler(results_handlers)
                    yield y.data

    # TODO: show submitted task and cached results in progress bar
    def map_reduce(self, iterable_input, reduce_fn, reduce_batch):
        '''
         applies the functions composition to each element of an input iterable,
         then reduces the results using another function.
         See the documentation for the class Farm for details.

         :param: iterable_input the iterable to map
         :param: reduce_fn functions that reduces mapped results. It must be associative and commutative
         :param: reduce_batch number of map results to compute before each call to reduce_fn
        '''

        if logger.isEnabledFor(logging.DEBUG):
            self.show_progress_bar = False

        iterable = grouper(tqdm.tqdm(iterable_input, desc="farm input", disable=not self.show_progress_bar),
                           self.batchsize)
        
        if self.nworkers == 1:
            logger.info("map-reduce: sequential version")
            out = None
            for y in grouper(map(self.parallel_process, iterable), reduce_batch):
                y = sum((res.data for res in y), [])
                logger.debug("map-reduce calling reduce_fn")
                out = reduce_fn(y + ([] if out is None else [out]))
            return out
        else:
            logger.info("map-reduce: parallel version ({} workers)".format(self.nworkers))
            logger.info("map-reduce: reduce batch: {}".format(reduce_batch))

            partial_reduce = functools.partial(wrap_reduce_function, reduce_fn)
            cached_results_tree = [[]]
            results_handlers: List[AsyncResult] = []
            with mp.Pool(self.nworkers) as pool:

                # get nworkers batches from the input iterable, submit them to the pool 
                for _, task in zip(range(self.nworkers), iterable):
                    handle = pool.apply_async(self.parallel_process, [task])
                    results_handlers.append(handle)

                logger.debug("map-reduce: submitted first {} tasks to the pool".format(self.nworkers))

                # iterate the remaining batches, get one batch at a time and submit it when a previous task is completed
                for task in iterable:
                    
                    y = self.get_result_and_delete_handler(results_handlers)
                    level = y.level

                    if len(cached_results_tree) == level:
                        cached_results_tree.append([])
                    cached_results_tree[level].extend(y.data)

                    h = pool.apply_async(self.parallel_process, [task])
                    results_handlers.append(h)
                    logger.debug("map-reduce collected {} results for level {}, cached results (by level): "
                                 "{}".format(len(y.data), level,
                                             list(map(lambda l: len(l), cached_results_tree))))

                    while len(cached_results_tree[level]) >= reduce_batch:
                        h = pool.apply_async(partial_reduce, [cached_results_tree[level], level+1])
                        cached_results_tree[level] = []
                        results_handlers.append(h)
                        logger.debug("map-reduce: submitted partial_reduce for level {}".format(level))
                        y = self.get_result_and_delete_handler(results_handlers)
                        level = y.level
                        if len(cached_results_tree) == level:
                            cached_results_tree.append([])
                        cached_results_tree[level].extend(y.data)
                        logger.debug("map-reduce collected {} results for level {}, cached results (by level): "
                                     "{}".format(len(y.data), level,
                                                 list(map(lambda l: len(l), cached_results_tree))))

                logger.debug("map-reduce: submitted all the input tasks")
                # wait until all the tasks are completed
                while len(results_handlers):
                    y = self.get_result_and_delete_handler(results_handlers)
                    cached_results_tree[0].extend(y.data)
                    logger.debug("map-reduce collected {} results (put in level 0 since it does not matter), "
                                 "cached results (by level): "
                                 "{}".format(len(y.data), list(map(lambda l: len(l), cached_results_tree))))

                logger.debug("map-reduce: finished collecting all tasks, calling last reduce_fn to reduce "
                             "the last {} cached results in all the levels "
                             "of the tree".format(len(sum(cached_results_tree, []))))
                return reduce_fn(sum(cached_results_tree, []))
