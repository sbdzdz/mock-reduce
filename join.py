import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
  order_id = record[1]
  mr.emit_intermediate(order_id, record)

def reducer(key, values):
  joined = []
  for v in values:
    if v[0] == 'order':
      joined = v+joined
    else:
      joined += v
  mr.emit(joined)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)