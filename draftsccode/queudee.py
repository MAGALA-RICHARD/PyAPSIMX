from multiprocessing import Pool, TimeoutError
import time
import os

a = [1,2,3,4,5,6,7,8,9,10]
def f(nu):
    return a[nu] *10

if __name__ == '__main__':
    # start 4 worker processes
    with Pool(processes=4) as pool:
        p = range(10)
        y = range(10)
        print(pool.map(f, p))
