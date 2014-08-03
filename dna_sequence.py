import MapReduce
import sys
import itertools

mr = MapReduce.MapReduce()

def mapper(record):
  seq = record[1]
  mr.emit_intermediate(seq[:-10], seq[:-10])

def reducer(key, list_of_values):
  for v in set(list_of_values):
    mr.emit((v))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)