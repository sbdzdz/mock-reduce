import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
  mr.emit_intermediate(record[0], 1)

def reducer(key, values):
  total = len(values)
  mr.emit((key, total))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)