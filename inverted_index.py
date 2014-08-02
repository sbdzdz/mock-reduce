import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
  key = record[0]
  value = record[1]
  text = value.split()
  for word in text:
    mr.emit_intermediate(word, key)

def reducer(key, values):
  ids = set()
  for v in values:
    ids.add(v)
  mr.emit((key, list(ids)))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)