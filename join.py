import MapReduce
import sys
import itertools
import operator

mr = MapReduce.MapReduce()

def mapper(record):
  order_id = record[1]
  mr.emit_intermediate(order_id, record)

def reducer(key, values):
  order = values[0]
  items = values[1:]
  for p in itertools.product([order], items):
    mr.emit(reduce(operator.add, p))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)