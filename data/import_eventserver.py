"""
Import sample data for classification engine
"""

import predictionio
import argparse

def import_events(client, file):
  f = open(file, 'r')
  count = 0
  print "Importing data..."
  for line in f:
    data = line.rstrip('\r\n').split(",")
    plan = data[0]
    attr = data[1].split(" ")
    client.create_event(
      event="$set",
      entity_type="user",
      entity_id=str(count), # use the count num as user ID
      properties= {
        "attr0" : int(attr[0]),
        "attr1" : int(attr[1]),
        "attr2" : int(attr[2]),
        "attr3" : int(attr[3]),
        "attr4" : int(attr[4]),
        "attr5" : int(attr[5]),
        "attr6" : int(attr[6]),
        "attr7" : int(attr[7]),
        "attr8" : int(attr[8]),
        "attr9" : int(attr[9]),
        "attr10" : int(attr[10]),
        "attr11" : int(attr[11]),
        "attr12" : int(attr[12]),
        "attr13" : int(attr[13]),
        "attr14" : int(attr[14]),
        "attr15" : int(attr[15]),
        "plan" : int(plan)
      }
    )
    count += 1
  f.close()
  print "%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for classification engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/data.txt")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
