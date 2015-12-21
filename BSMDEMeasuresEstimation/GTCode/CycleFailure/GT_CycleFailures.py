#standard
import argparse
import os

#local
from GTFileReader import Trajectories
from readlinks import read_intersection_file, read_intersection_cycle_file, read_signal_controllers_file, read_greentimes_file
from TCACore import Timer



QUEUE_START_SPEED = 0.0 
QUEUE_FOLLOWING_SPEED = 10.0 #ft/s 
QUEUE_HEADWAY_DISTANCE = 20.0 #ft
QUEUE_DISTANCE_WITHIN_STOP_POINT = 20 #ft
CONVERT_METERS_TO_FT = 100 / 2.54 / 12 
t = Timer(enabled=True)

def run_gt_cyclefailure_new(trj, bnlinks, int_map, green_times, output_name):

    output = []
    with open(output_name, 'wb') as out_f:
        out_f.write('intersection,lane_group,green_starttime,next_greentime\n')

    max_queues = {}
    for bottleneck_loc in bnlinks:
        max_queues[bottleneck_loc] = {}
        for lane_group in bnlinks[bottleneck_loc].keys():
            max_queues[bottleneck_loc][lane_group] = {'link' : 0, 'lane' : 0, 'max_queue_length' : 0.0, 'queued_vehicles': []}

    with open(output_name, 'a') as out_f:

        tp_count  = 0
        tp_start = True
        
        # read through trajectory file time period by time period
        for tp, veh_list in trj.read():

            # If first time period, set the current green time for each intersection lane_group to the one that matches the vehicle trajectory data best
            if tp_start:
              for intersection in int_map.keys():
                for lane_group in int_map[intersection].keys():
                  controller_num = int_map[intersection][lane_group]['controller_num']
                  signal_grp = int_map[intersection][lane_group]['signal_group']
                  # Loop through possible green times to find the one valid for the first tp 
                  for greentime in green_times[controller_num][signal_grp]:
                      if tp <= greentime:
                          int_map[intersection][lane_group]['current_greentime'] = greentime
                          break
                  int_map[intersection][lane_group]['current_redtime'] = red_times[controller_num][signal_grp][green_times[controller_num][signal_grp].index(int_map[intersection][lane_group]['current_greentime'])]

            tp_start = False

            # Loop through each intersection_approach and lane_group
            for intersection_approach in int_map.keys():
              for lane_group in int_map[intersection_approach].keys():
                controller_num = int_map[intersection_approach][lane_group]['controller_num']
                signal_grp = int_map[intersection_approach][lane_group]['signal_group']

                # Set the next green time to next greentimetime in the list or the last time step of the simulation
                current_greentime = int_map[intersection_approach][lane_group]['current_greentime']
                current_redtime = int_map[intersection_approach][lane_group]['current_redtime']
                num_greentimes = len(green_times[controller_num][signal_grp])


                if green_times[controller_num][signal_grp].index(current_greentime) + 1 >= num_greentimes:
                  next_greentime = LAST_TIME_STEP
                else:
                  next_greentime = green_times[controller_num][signal_grp][green_times[controller_num][signal_grp].index(current_greentime)+1]
                # There are no more valid greentimes
                if current_greentime == LAST_TIME_STEP or current_redtime == LAST_TIME_STEP:
                  continue
                
                # Check each intersection_approach,lane_group green time to see if a new green time has started
                # If a new cycle (greentime) is started, determine if any of the vehicles from the previous start of the green time are still in queue (cycle failure)
                if float(tp) >= float(next_greentime) + 0.1:
                  for vehid in int_map[intersection_approach][lane_group]['current_greentime_ids']:
                    if vehid in int_map[intersection_approach][lane_group]['current_redtime_ids']:

                      # Find the cycle start time for that intersection to include in the output
                      out_f.write("%s,%s,%.1f,%.1f\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],next_greentime))
                      break

                  # Reset active cycle ids and current greentime start time
                  int_map[intersection_approach][lane_group]['current_greentime'] = float(next_greentime)
                  try:
                    int_map[intersection_approach][lane_group]['current_redtime'] = float(red_times[controller_num][signal_grp][green_times[controller_num][signal_grp].index(next_greentime)])
                  except:
                    int_map[intersection_approach][lane_group]['current_redtime'] = LAST_TIME_STEP
                    pass
                  int_map[intersection_approach][lane_group]['current_greentime_ids'] = int_map[intersection_approach][lane_group]['current_redtime_ids']
                  int_map[intersection_approach][lane_group]['current_redtime_ids'] = []


            tp_count +=1

            if tp_count % 1000 ==0:
                print tp

            # Known Queue Step #3 : Loop through each bottleneck location and lane
            for bottleneck_loc in bnlinks:

                for lane_group in bnlinks[bottleneck_loc].keys():

                    # Skip to the next lane group or bottlenck if the current timestep is not a green time start
                    if float(tp) > float(int_map[bottleneck_loc][lane_group]['current_greentime']) and float(tp) < float(int_map[bottleneck_loc][lane_group]['current_redtime']):
                      continue

                    # Known Queue Step #4: Identify all vehicles on that link in an array with their position (ft), speed, and vehicle length
                    roadway_veh = get_roadway_vehicles(bottleneck_loc, lane_group, bnlinks, veh_list)

                    # Known Queue Step #5: Sort arrays by increasing distance from the end of the link
                    roadway_veh = sorted(roadway_veh, key=lambda k: k['x'])

                    if len(roadway_veh) > 0:
                        q_c = 0
                        q_len = 0
                        first_veh = None
                        last_veh = None
                        leader_veh = None
                        q_counter = 1
                        veh_count = 0
                        queued_vehicles = []

                        # Known Queue Step #6: Starting with the vehicle closest to the end of the link
                        for veh in roadway_veh:
                          veh_count += 1

                          if q_c == 0:
                                # Known Queue Step #7: Look for a motionless vehicle within range of stop bar
                                if (veh['v'] == QUEUE_START_SPEED) and (veh['stopbar_x'] <= QUEUE_DISTANCE_WITHIN_STOP_POINT):
                                    q_c = 1
                                    first_veh = last_veh = leader_veh = veh
                                    queued_vehicles.append(str(veh['vehid']))

                          elif leader_veh != None and (veh['v'] <= QUEUE_FOLLOWING_SPEED) and \
                            (( veh['x'] -  (leader_veh['x'] + leader_veh['Length'])) <= QUEUE_HEADWAY_DISTANCE):
                              q_c +=1
                              last_veh = leader_veh = veh
                              queued_vehicles.append(str(veh['vehid']))

                          #if first vehicle out of queue
                          elif q_c > 0:
                              q_len = last_veh['stopbar_x']
                              max_queues = set_max_queue(q_len, bottleneck_loc, lane_group, max_queues, queued_vehicles)
                              break
                        if float(tp) == float(int_map[bottleneck_loc][lane_group]['current_greentime']):
                          int_map[bottleneck_loc][lane_group]['current_greentime_ids'] = max_queues[bottleneck_loc][lane_group]['queued_vehicles']
                          max_queues[bottleneck_loc][lane_group] = {'link' : 0, 'lane' : 0, 'max_queue_length' : 0.0, 'queued_vehicles': []}
                        controller_num = int_map[bottleneck_loc][lane_group]['controller_num']
                        signal_grp = int_map[bottleneck_loc][lane_group]['signal_group']
                        if green_times[controller_num][signal_grp].index(int_map[bottleneck_loc][lane_group]['current_greentime']) + 1 < len(green_times[controller_num][signal_grp]):
                          if float(tp) == float(green_times[controller_num][signal_grp][green_times[controller_num][signal_grp].index(int_map[bottleneck_loc][lane_group]['current_greentime'])+1]):
                            int_map[bottleneck_loc][lane_group]['current_redtime_ids'] = max_queues[bottleneck_loc][lane_group]['queued_vehicles']
                            max_queues[bottleneck_loc][lane_group] = {'link' : 0, 'lane' : 0, 'max_queue_length' : 0.0, 'queued_vehicles': []}


def set_max_queue(q_length, bottleneck, lane_group, max_queues, queued_vehicles):
    #if float(max_queues[bottleneck][lane_group]['max_queue_length']) < q_length:
     #   max_queues[bottleneck][lane_group]['max_queue_length'] = q_length
    max_queues[bottleneck][lane_group]['queued_vehicles'] = max_queues[bottleneck][lane_group]['queued_vehicles'] + list(set(queued_vehicles) - set(max_queues[bottleneck][lane_group]['queued_vehicles']))

    return max_queues


def write_output(out_f, output):
  if len(output) > 0:
      print 'Writing to file'
      for line in output:
           out_f.write(','.join(line) + '\n')

def distance_between(origin_x, origin_y, destination_x, destination_y):
    return ((origin_x - destination_x)**2 + (origin_y - destination_y)**2)**.5

def get_roadway_vehicles(roadway, lane_group, bnlinks, veh_list):


    roadway_veh = []
    

    for link_position, link_data in enumerate(bnlinks[roadway][lane_group]['link_list']):
        link_num, link_lane = link_data[0], link_data[1]
        # dis_stop = 0

        # # If not the first link in the roadway, find the summed distance of all prior links
        # if link_position != 0:
        #     for i in reversed(range(link_position)):
        #         dis_stop += bnlinks[roadway][lane_group][i][2]

        for veh in veh_list:
            if (veh['Link'] == link_num) and (veh['Lane'] == link_lane):

                #change distance from end of link to distance to stop point
                veh['x'] = distance_between(bnlinks[roadway][lane_group]['link_x'], bnlinks[roadway][lane_group]['link_y'], veh['World_x'], veh['World_y'])
                veh['stopbar_x'] = distance_between(bnlinks[roadway][lane_group]['stopbar_x'], bnlinks[roadway][lane_group]['stopbar_y'], veh['World_x'], veh['World_y'])
                roadway_veh.append(veh)

    return roadway_veh

def read_superlink_file(filename):

    superlinks = {}

    for line in open (filename):
        row = line.strip().split(',')

        if row[0] not in superlinks.keys():
            superlinks[row[0]] = {} # Key is superlink/bottleneck name
        if row[1] not in superlinks[row[0]].keys():
            superlinks[row[0]][row[1]] = {}
            superlinks[row[0]][row[1]]['link_list'] = []

        superlinks[row[0]][row[1]]['stopbar_x'] = float(row[2]) * CONVERT_METERS_TO_FT
        superlinks[row[0]][row[1]]['stopbar_y'] = float(row[3]) * CONVERT_METERS_TO_FT
        superlinks[row[0]][row[1]]['link_x'] = float(row[4]) * CONVERT_METERS_TO_FT
        superlinks[row[0]][row[1]]['link_y'] = float(row[5]) * CONVERT_METERS_TO_FT


        for col in range(6,len(row), 3):
            if row[col] == '':
                break
            superlinks[row[0]][row[1]]['link_list'].append( (int(row[col]), int(row[col+1]), float(row[col+2]))  ) # list in the order: link, lane, length

    #link_dict = all_links(superlinks)

    return superlinks#, link_dict

t.start('main')

parser = argparse.ArgumentParser(description='GT program for reading in fzp files and producing Cycle Failure values')
parser.add_argument('trj_filename', help = 'FZP file of vehicle trajectories') 
parser.add_argument('bottleneck_filename', help = 'CSV file of bottlenecks')  
parser.add_argument('signal_controllers_filename', help = 'Map of intersection approach and lane group to signal head')
parser.add_argument('greentimes_filename', help = 'Signal controller VISSIM output (.lsa)') 
parser.add_argument('timestep', help = 'Last time of data in seconds') 
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

trj = Trajectories(filename=args.trj_filename)

bnlinks = read_superlink_file(args.bottleneck_filename)
int_map = read_signal_controllers_file(args.signal_controllers_filename)
green_times, red_times = read_greentimes_file(args.greentimes_filename)
LAST_TIME_STEP = args.timestep
if args.out:
    out_file = dir_path + '/' + args.out
else:
    out_file = dir_path + '/cyclefailure_gt.csv'

run_gt_cyclefailure_new(trj, bnlinks, int_map, green_times, out_file)

t.stop('main')
print t['main']