import MapReduce
import sys
import itertools

mr = MapReduce.MapReduce()

def mapper(record):
  record.sort()
  mr.emit_intermediate(record[0], record[1])

def reducer(key, list_of_values):
  for v in list_of_values:
    if list_of_values.count(v) == 1:
      mr.emit((key, v))
      mr.emit((v, key))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)