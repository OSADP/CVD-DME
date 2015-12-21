


def all_links(bnlinks):
     all_key_links = []

     for bottleneck in bnlinks.keys():
        for i, stem in enumerate(bnlinks[bottleneck].keys()):
            for lnk in bnlinks[bottleneck][stem]:
                all_key_links.append(lnk[0])

     return all_key_links


def read_link_file(filename):

    bnlinks = {}

    for line in open (filename):
        row = line.strip().split(',')

        if row[0] not in bnlinks.keys():
            bnlinks[row[0]] = {}

        if row[1] not in bnlinks[row[0]].keys():
            bnlinks[row[0]][row[1]] = []

        for col in range(2,len(row),3):
            if row[col] == '':
                break
            bnlinks[row[0]][row[1]].append( (int(row[col]), int(row[col+1]), float(row[col+2]))  )

    keylinks = all_links(bnlinks)

    return bnlinks, keylinks

def read_intersection_file(filename):

    inlinks = {}

    for line in open (filename):
        row = line.split(',')

        if str(row[1]).strip() not in inlinks.keys():
            inlinks[str(row[1]).strip()] = str(row[0])

    return inlinks

def read_intersection_cycle_file(filename):

  cycles = {}

  for line in open(filename):
    row = line.split(',')
    cycles[row[0]] = []
    for time in row[1:]:
      cycles[row[0]].append(float(time))
  return cycles

def read_greentimes_file(lsa_file):
  green_times = {}
  #green_times[controller_num][signal_group] = [list of greentimes]

  for lsa_line in open(lsa_file):
    lsa_row = lsa_line.split(';')

    if lsa_row[2].strip() not in green_times.keys():
      green_times[lsa_row[2].strip()] = {}
    if lsa_row[3].strip() not in green_times[lsa_row[2].strip()].keys():
      green_times[lsa_row[2].strip()][lsa_row[3].strip()] = []
    if lsa_row[4].strip() == 'green':
      green_times[lsa_row[2].strip()][lsa_row[3].strip()].append(lsa_row[0].strip())

  return green_times


# Read in a csv with the following data in order: intersection name, stem, controller number, signal group number
def read_signal_controllers_file(filename):
  intersections = {}
  #intersections[name][stem]={controllernum,signalgroup,prev ids, current greentime, current ids}

  for line in open(filename):
    row = line.split(',')

    if row[0] not in intersections.keys():
      intersections[row[0]] = {}
    if row[1] not in intersections[row[0]].keys():
      intersections[row[0]][row[1]] = {'controller_num': row[2],
                                       'signal_group': row[3],
                                       'prev_greentime_ids':[],
                                       'current_greentime':'1.0',
                                       'current_greentime_ids':[],
                                        }
  return intersections

def read_routes(filename):
  routes = {}

  for line in open(filename):

        row = line.strip().split(',')
        route_group = row[0]
        route = row[1]
        route_origin = row[2].strip()
        route_destination = row[3].strip()

        if route_group not in routes.keys():
          routes[route_group] = {}

        if route not in routes[route_group].keys():
          routes[route_group][route] = {'route_origin': route_origin,
                                        'route_destination': route_destination,
                                       }
  return routes

def read_traveltimes_file(filename):
  travel_times = {}

  for line in open(filename):
    row = line.strip().split(',')
    route_group = row[0]
    route_num = row[1]
    minute = row[2]
    avg_ttime = row[3]
    if route_group not in travel_times:
      travel_times[route_group] = {}
    if route_num not in travel_times[route_group]:
      travel_times[route_group][route_num] = {}
    if minute not in travel_times[route_group][route_num]:
      travel_times[route_group][route_num][minute] = float(avg_ttime)
    else:
      print 'Error'
      print route_group
      print route_num
      print minute

  return travel_times

def read_speed_limit(filename):
  speed_limits = {}

  for line in open(filename):
    row = line.strip().split(',')
    link = row[0].strip()
    speed = float(row[1])
    speed_limits[link] = speed

  return speed_limits

def read_link_length(filename):
  link_lengths = {}

  for line in open(filename):
    row = line.strip().split(',')
    link = row[0].strip()
    length = float(row[1])
    link_lengths[link] = length

  return link_lengths

def read_full_routes(filename):
  routes = {}

  for line in open(filename):
    row = line.strip().split(',')
    route_group = row[0]
    route = row[1]

    if route_group not in routes.keys():
        routes[route_group] = {}

    if route not in routes[route_group].keys():
        routes[route_group][route] = []

    for col in range(2,len(row)):
        if row[col] == '':
            break
        routes[route_group][route].append(row[col].strip())

  return routes

def read_endpoints(filename):
  endpoints = {}

  for line in open(filename):
    row = line.strip().split(',')
    link = row[0].strip()

    if link not in endpoints.keys():
      endpoints[link] = {}

    endpoints[link]["x"] = float(row[1])
    endpoints[link]["y"] = float(row[2])

  return endpoints

###########################################
# file = r'D:\Data\Tasks\FHWA\Current\DCM_Contract\BSM Emulator\GT_Coding\superlinks_list_VanNess.csv'
#
# read_link_file(file)
#
# super_links, all_key_links = read_link_file(file)
#
# print all_key_links
# print super_links

# file = r'C:\Users\M29565\Documents\Projects\tca\GT_code\CycleFailure\superlinks_list_VanNess_intersections.csv'
# int_links = read_intersection_file(file)
# print int_links
# intersection_map = {}
# for int_name in int_links.values():
#     if int_name not in intersection_map.keys():
#         intersection_map[int_name] = {'prev_cycle_ids':[1,2],
#                                       'current_cycle':0.0,
#                                       'cycle_ids':[]
#                                       }
# for int_name in int_links.values():
#   intersection_map[int_name]['prev_cycle_ids'].extend([2,3])
# print intersection_map

# file = r'C:\Users\M29565\Documents\Projects\tca\GT_code\CycleFailure\intersection_cycle_times.csv'
# cycles = read_intersection_cycle_file(file)
# print cycles

# file = r'C:\Users\M29565\Documents\Projects\tca\GT_code\CycleFailure\medDemand_test.lsa'
# green_times = read_greentimes_file(file)
# print green_times

# file = r'C:\Users\M29565\Documents\Projects\tca\GT_code\CycleFailure\vanness_greentimes.csv'
# cycles = read_cycle_file(file)
# print cycles