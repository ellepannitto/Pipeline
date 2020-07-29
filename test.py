import unittest
import random
import os
import time
import Farm

def double(x_list):
    for x in x_list:
        # print(os.getpid(), time.time(), "double", x)
        for i in range(random.randint(500000, 1000000)):
            pass
        # print(os.getpid(), time.time(), "finish double", x)
        yield [2 * x]


def increment(x_list):
    for x in x_list:
        # print(os.getpid(), time.time(), "increment", x)
        for i in range(random.randint(700000, 1300000)):
            pass
        # print(os.getpid(), time.time(), "finished increment", x)
        yield [x + 1]

def expensive_sum (x_list):
    out = 0
    for x in x_list:
        for i in range(random.randint(700000, 1300000)):
            pass
        out += x
    return out

class TestFarm(unittest.TestCase):

    def test_simple(self):
        farm = Farm.Farm([double, increment, double], 1, show_progress_bar = False)
        exp_results = {2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46}
        results = set ( y[0] for y in farm.map (range(12)) )
        self.assertEqual (results, exp_results, "farm does not work")       

    def test_batch_size(self):
        farm = Farm.Farm([double, increment, double], 1, 2, show_progress_bar = False)

        ys = sum (farm.map(range(12)), [])
        ts = [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46]

        self.assertEqual (ys, ts, "farm does not work")

        for y in farm.map (range(12)):
            self.assertEqual (len(y), 2, "farm with batchsize=2 does not return 2 results")

    def test_workers(self):
        farm = Farm.Farm([double, increment, double], 2, show_progress_bar = False)
        exp_results = {2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46}
        results = set ( y[0] for y in farm.map (range(12)) )
        self.assertEqual (results, exp_results, "farm does not work")

    def test_batch_size_and_workers(self):
        farm = Farm.Farm([double, increment, double], 2, 2, show_progress_bar = False)

        ys = set(sum (farm.map(range(12)), []))
        ts = {2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46}

        self.assertEqual (ys, ts, "farm does not work")

        for y in farm.map (range(12)):
            self.assertEqual (len(y), 2, "farm with batchsize=2 does not return 2 results")

    def test_map_reduce (self):
        farm = Farm.Farm([double, increment, double], 1, show_progress_bar = False)
        exp_result = 288
        result = farm.map_reduce (range(12), expensive_sum, 5)
        self.assertEqual (result, exp_result, "map reduce does not work")       

    def test_map_reduce_workers (self):
        farm = Farm.Farm([double, increment, double], 2, show_progress_bar = False)
        exp_result = 288
        result = farm.map_reduce (range(12), expensive_sum, 3)
        self.assertEqual (result, exp_result, "map reduce does not work")       
       
    def test_map_reduce_workers_and_batchsize (self):
        farm = Farm.Farm([double, increment, double], 2, 2, show_progress_bar = False)
        exp_result = 288
        result = farm.map_reduce (range(12), expensive_sum, 3)
        self.assertEqual (result, exp_result, "map reduce does not work")       
       
        
if __name__ == "__main__":
    unittest.main()    
