import multiprocessing as mp
import functools
import random
import os
import time

def double (x):
    # print(os.getpid(), time.time(), "double", x)
    for i in range(random.randint(50000000, 100000000)):
        pass
    # print(os.getpid(), time.time(), "finish double", x)
    yield 2*x

def increment(x):
    # print(os.getpid(), time.time(), "increment", x)
    for i in range(random.randint(70000000, 130000000)):
        pass
    # print(os.getpid(), time.time(), "finished increment", x)
    yield x+1

def parallel_process (f, in_q, out_q):
    while True:
        x = in_q.get()
        if x is None:
            break

        for y in f(x):
            out_q.put(y)


def main():

    n_workers = 8

    in_q = mp.Queue()
    mid_q = mp.Queue()
    out_q = mp.Queue()

    pool_double = mp.Pool(n_workers, initializer=parallel_process, initargs=(double, in_q, mid_q))
    pool_increment = mp.Pool(n_workers, initializer=parallel_process, initargs=(increment, mid_q, out_q))

    inputs = range(10)

    for x in inputs:
        in_q.put(x)

    for _ in range(n_workers):  # tell workers we're done
        in_q.put(None)

    pool_double.close()
    pool_double.join()

    for _ in range(n_workers):
        mid_q.put(None)

    pool_increment.close()
    pool_increment.join()

    out_q.put (None)

    while True:
        y = out_q.get()
        if y==None:
            break
        print ("Result", y)

    print ("Exit")

if __name__ == "__main__":
    main()