import logging
import tqdm

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

    def __init__(self, list_of_functions, list_of_workers, batches=None, show_progress_bar=True):
        self.functions = list_of_functions
        self.workers = list_of_workers
        self.show_progress_bar = show_progress_bar

        if batches is None:
            batches = [1]*(len(list_of_functions)+1)
        self.batches = batches

        assert len(self.functions) == len(self.workers), "number of functions does not match number of workers"
        assert len(self.functions) == len(self.batches)-1, "number of functions does not match number of batches-1"

    def parallel_process(self, x):

        for func in self.functions:
            x = sum(func(x), [])
        return x

    def run(self, iterable_input):
        iterable = tqdm.tqdm(grouper(iterable_input, self.batches[0]), desc="farm input", disable=not self.show_progress_bar)
        for y in map (self.parallel_process, iterable):
            yield y
