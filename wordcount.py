import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
  text = record[1]
  for w in text.split():
    mr.emit_intermediate(w, 1)

def reducer(key, values):
  total = len(values)
  mr.emit((key, total))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
