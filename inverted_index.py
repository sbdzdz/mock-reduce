import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
  index = record[0]
  text = record[1]
  for word in text.split():
    mr.emit_intermediate(word, index)

def reducer(key, values):
  indices = set()
  for v in values:
    indices.add(v)
  mr.emit((key, list(indices)))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)