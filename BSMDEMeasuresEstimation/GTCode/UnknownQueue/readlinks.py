


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

def read_stopline_distances_file(filename):
    bottlenecks = {}

    for line in open(filename):

        row = line.strip().split(',')
        bottleneck_name = row[0]
        lane_group = row[1]
        stop_dist = row[4]

        if bottleneck_name not in bottlenecks.keys():
            bottlenecks[bottleneck_name] = {}

        if lane_group not in bottlenecks[bottleneck_name].keys():
            bottlenecks[bottleneck_name][lane_group] = stop_dist

    return bottlenecks


def read_endpoints(filename):
  endpoints = {}

  for line in open(filename):
    row = line.strip().split(',')
    link = float(row[0])

    if link not in endpoints.keys():
      endpoints[link] = {}

    endpoints[link]["x"] = float(row[1]) * (100 / 2.54 / 12)
    endpoints[link]["y"] = float(row[2]) * (100 / 2.54 / 12)

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