from multiprocessing import Process, Queue

def f(q):
    q.put([42, None, 'hello'])

if __name__ == '__main__':
    q = Queue()
    p = Process(target=f, args=(q,))
    p.start()
    print(q.get())    # prints "[42, None, 'hello']"
    p.join()
def makedf(listitem):
  import pandas
  free = []
  for i in listitem:
    if i !=None:
     free.append(i)
  df = pandas.DataFrame.from_dict(free)
  return df
