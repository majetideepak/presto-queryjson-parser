#!/usr/bin/env python3

# Python program to convert
# json to parquet tables
 
import json
import sys
import os
from optparse import OptionParser

def create_parser():
  parser = OptionParser(prog='queryjson', usage='usage: %prog [options] path', description="Analyze Presto UI Query Json files. If multiple query files are provided, the output is ordered by execution time longest to shortest.")
  parser.add_option('--stagestate', action='store_true', default=False, help='Collect and print stage status information')
  parser.add_option('--opwall', action='store', default=0, type="int", dest="opwall_s", help='Minimum Operator Wall time in seconds to show operator details (Default: 0)')
  parser.add_option('--sortby', action='store', default='getOutputWall', type="string", dest="sort_key", help='Sort field (Default: \'getOutputWall\'. Other fields: \'addInputWall\', \'blockedWall\')')
  parser.add_option('--runtimestats', action='store_true', default='False', help='Get only the runtime stats where max > 1 second')
  parser.add_option('--printstages', action='store_true', default='False', help='Print the output stage fragments (plan)')
  return parser

def time_val(tstr):
  time = 0
  if (tstr[-2:] == 'ns' or tstr[-2:] == 'ms' or tstr[-2:] == 'us') :
    time = 0
  elif (tstr[-1] == 's') :
    time = float(tstr[:-1])
  elif (tstr[-1] == 'm') :
    wh = int(tstr[:-4])
    part = int(tstr[-3:-1]) 
    time = wh * 60 + part * 60 / 100
  elif (tstr[-1] == 'h') :
    wh = int(tstr[:-4])
    part = int(tstr[-3:-1])
    time = wh * 60 * 60 + part * 60 * 60 / 100
  return time


def printFailed(queries):
  print('Failed Queries:')
  for query in queries:
    print(query)
  print('')

def printSorted(queries, sort_key):
  sorted_queries = sorted(queries, key = lambda x: x[0], reverse = True) 
  print('Sorted Queries')
  for query in sorted_queries:
    print('execTime : ' + query[1]['execTime'] + '(' + str(query[0])  + 's)')
    print('file : ' + query[1]['file'])
    print('totalTasks : ' + str(query[1]['totalTasks']))
    print('peakRunningTasks : ' + str(query[1]['peakRunningTasks']))
    print('totalCpuTime : ' + str(query[1]['totalCpuTime']))
    print('totalBlockedTime : ' + query[1]['totalBlockedTime'])
    print('shuffledDataSize : ' + query[1]['shuffledDataSize'])
    if ('stageState' in query[1]):
      print('Stage State')
      for state in query[1]['stageState']:
        print(state[1])
    if (len(query[1]['opSummaries']) > 0):
      print('Top Operators sorted by ' + sort_key)
      for summary in query[1]['opSummaries']:
        print(summary[1])
    else:
      print("Skipping Operators. OutputWall too small for operator collection.")

def printPlan(plan):
    print(plan['id'] + " " + plan['name'])
    print(plan['identifier'])
    if not plan['details']:
      print(plan['details'])
    for child in plan['children']:
      printPlan(child)

def printstages(outputStage):
    print(outputStage['stageId'])
    plan = json.loads(outputStage['plan']['jsonRepresentation'])
    printPlan(plan)
    for stage in outputStage['subStages']:
        printstages(stage)

def printRuntimeStats(stats):
  for key, value in stats.items():
    if (key.endswith('runningGetOutputWallNanos') and  value['unit'] == 'NANO' and value['max'] / 1000000000 > 1):
      print(key + ": "  + str(value['sum'] / 1000000000) + " " +
                          str(value['min'] / 1000000000) + " " +
                          str(value['max'] / 1000000000))

def main():
  parser = create_parser()
  (options, args) = parser.parse_args()

  if (len(args) != 1):
      sys.exit("Specify the input json file or directory.")

  jsonfiles = []

  if os.path.isfile(args[0]):
    jsonfiles.append(args[0])
  else:
    for subdir, dirs, files in os.walk(args[0]):
      for file in files:
          jsonfiles.append(os.path.join(subdir, file))

  failed = []
  queries = []
  for file in jsonfiles:
    jsonfile = open(file)
    data = json.load(jsonfile)

    root = {}
    root['file'] = file;
    if (data['state'] == 'FAILED'):
      failed.append(file)

    if (options.runtimestats) :
      printRuntimeStats(data['queryStats']['runtimeStats'])

    if (options.printstages) :
      printstages(data['outputStage'])

    if (options.stagestate) :
      stages = []
      states = []
      stages.append(data['outputStage'])
      while (len(stages) > 0):
        stage = stages.pop(0)
        offset = stage['stageId'].index('.')
        stageId = stage['stageId'][offset + 1:]
        state = stage['latestAttemptExecutionInfo']['state']
        states.append((int(stageId), {"Stage " + stageId + ' : ' + state}))
        stages += stage['subStages']
      sorted_states = sorted(states, key = lambda x: x[0])
      root['stageState'] = sorted_states

    root['query'] = data['query'];
    queryStats = data['queryStats'];
    root['execTime'] = queryStats['executionTime'];
    timeVal = time_val(root['execTime'])

    root['totalTasks'] = queryStats['totalTasks'];
    root['peakRunningTasks'] = queryStats['peakRunningTasks'];
    root['totalDrivers'] = queryStats['totalDrivers'];
    root['totalCpuTime'] = queryStats['totalCpuTime'];
    root['totalBlockedTime'] = queryStats['totalBlockedTime'];
    root['shuffledDataSize'] = queryStats['shuffledDataSize']; 

    opSummaries = queryStats['operatorSummaries'];
    summaries = []
    key = options.sort_key
    for s in opSummaries:
      opVal = time_val(s[key])
      if (opVal > options.opwall_s) :
        summaries.append((time_val(s[key]),
                          {'stage':s['stageId'],
                          'opName':s['operatorType'],
                          'numDrivers':s['totalDrivers'],
                          'outputWall':s['getOutputWall'],
                          'inputWall':s['addInputWall'],
                          'blockedWall':s['blockedWall'],
                          }));
    
    sorted_summaries = sorted(summaries, key = lambda x: x[0], reverse = True)
    root['opSummaries'] = sorted_summaries;
    jsonfile.close()
    queries.append((timeVal, root));

  if (len(failed) > 0):
    printFailed(failed)

  printSorted(queries, options.sort_key)

if __name__ == '__main__':
    main()
