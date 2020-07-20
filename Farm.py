import logging
import tqdm
import multiprocessing as mp

import math

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


class Farm:

    def __init__(self, list_of_functions, nworkers, batchsize=1, show_progress_bar=True):
        self.functions = list_of_functions
        self.nworkers = nworkers
        self.show_progress_bar = show_progress_bar
        self.batchsize = batchsize

        assert batchsize>0, "batchsize must be greater than zero"
        assert nworkers>0, "nworkers must be greater than zero"

        
    def parallel_process(self, x):

        for func in self.functions:
            x = sum(func(x), [])
        return x

    def run(self, iterable_input):

        iterable = grouper(tqdm.tqdm(iterable_input, desc="farm input", disable=not self.show_progress_bar), self.batchsize)
        
        if self.nworkers == 1:
            for y in map (self.parallel_process, iterable):
                yield y
        else:

            with mp.Pool (self.nworkers) as pool:

                for y in pool.imap (self.parallel_process, iterable):
                    yield y
