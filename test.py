import unittest
import random
import os
import time
import Pipeline
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

class State:
    def __init__(self):
        self.received_items=0
        self.sum_items = 0

    def fun(self, x_list):
        for x in x_list:
            self.received_items += 1
            self.sum_items += x
            yield [self.sum_items]

@unittest.skip("")
class TestPipeline(unittest.TestCase):

    def test_simple(self):
        pipeline = Pipeline.Pipeline([double, increment, double], [1, 1, 1])

        for y, t in zip (pipeline.run(range(12)), [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46]):
            self.assertEqual(y[0], t, "pipeline does not work")

    def test_state(self):

        state_class = State()
        pipeline = Pipeline.Pipeline([double, increment, double, state_class.fun], [1, 1, 1, 1])

        for y, t in zip (pipeline.run(range(12)), [2, 8, 18, 32, 50, 72, 98, 128, 162, 200, 242, 288]):
            self.assertEqual(y[0], t, "pipeline does not work")

    def test_batch_size(self):
        pipeline = Pipeline.Pipeline([double, increment, double], [1, 1, 1], [3,3,2,1])

        for y, t in zip (pipeline.run(range(12)), [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46]):
            self.assertEqual(y[0], t, "pipeline does not work")


class TestFarm(unittest.TestCase):

    def test_simple(self):
        farm = Farm.Farm([double, increment, double], [1, 1, 1])

        for y, t in zip (farm.run(range(12)), [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46]):
            self.assertEqual(y[0], t, "pipeline does not work")

    def test_state(self):

        state_class = State()
        farm = Farm.Farm([double, increment, double, state_class.fun], [1, 1, 1, 1])

        for y, t in zip (farm.run(range(12)), [2, 8, 18, 32, 50, 72, 98, 128, 162, 200, 242, 288]):
            self.assertEqual(y[0], t, "pipeline does not work")

    def test_batch_size(self):
        farm = Farm.Farm([double, increment, double], [1, 1, 1], [1,3,2,1])

        for y, t in zip (farm.run(range(12)), [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46]):
            self.assertEqual(y[0], t, "pipeline does not work")


if __name__ == "__main__":
    unittest.main()