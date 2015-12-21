#standard
import argparse
import datetime
import os

#local
from GTFileReader import Trajectories
from readlinks import read_link_file,read_endpoints
from TCACore import Timer



QUEUE_START_SPEED = 0.0 
QUEUE_FOLLOWING_SPEED = 10.0 #ft/s 
QUEUE_STOPPED_VEHICLE_HEADWAY_DISTANCE = 40.0 #ft
QUEUE_HEADWAY_DISTANCE = 20.0 #ft
START_HOUR = 17
SECONDS_INTERVAL = 120
CONVERT_METERS_TO_FT = 100 / 2.54 / 12 
link_positions = {}
t = Timer(enabled=False)




def run_gt_unknown_queuing_new(trj, bnlinks, unknown_queue_output_name, max_queue_output_name):


    unknown_queue_output = []

    # For Step #7: Printing max queues for each minute
    max_output = []
    max_queues = {}
    start_time = datetime.datetime(100,1,1,int(START_HOUR),0,0)

    for bottleneck_loc in bnlinks:
        max_queues[bottleneck_loc] = {'link' : 0, 'stop_position': 0.0, 'queue_count' : 0, 'max_queue_length' : 0.0, 'queued_vehicles': [] }

    with open(max_queue_output_name, 'wb') as out_f:
        out_f.write('time,bottleneck,max_queue_length,max_queue_count\n')

    with open(unknown_queue_output_name, 'wb') as out_f:
        out_f.write('time,bottleneck,lane_group,queue_length,queue_count\n')

    with open(unknown_queue_output_name, 'a') as out_f:

        tp_count  = 0

        #Unknown Queue Step #1: Read through file time period by time period
        for tp, veh_list in trj.read():

            tp_count +=1

            if tp_count % 1000 ==0:
                print tp

            # Unknown Queue Step #2 and #3: Loop through each link and lane group
            for roadway in bnlinks:
                for lane_group in bnlinks[roadway].keys():

                    # Unknown Queue Step #4: Identify all vehicles on that link in an array with their position (ft), speed, and vehicle length
                    roadway_veh = get_roadway_vehicles(roadway, lane_group, bnlinks, veh_list)

                    # Unknown Queue Step #5: Sort arrays by increasing distance from the end of the link
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

                        # Unknown Queue Step #6: Starting with the vehicle closest to the end of the link, look for a motionless vehicle
                        for veh in roadway_veh:
                            veh_count += 1
                            if q_c == 0:
                                if (veh['v'] == QUEUE_START_SPEED):
                                    q_c = 1
                                    first_veh = last_veh = leader_veh = veh
                                    q_len = veh['Length']
                                    queued_vehicles.append(int(veh['vehid']))

                            # Cycle through remaining vehicles on the link, determining if they are in queue behind motionless vehicle 
                            elif leader_veh != None and (veh['v'] == QUEUE_START_SPEED) and ((veh['x'] - (leader_veh['x'] + leader_veh['Length'])) <= QUEUE_STOPPED_VEHICLE_HEADWAY_DISTANCE):
                                q_c += 1
                                last_veh = leader_veh = veh
                                queued_vehicles.append(int(veh['vehid']))

                            elif leader_veh != None and (veh['v'] <= QUEUE_FOLLOWING_SPEED) and \
                              (( veh['x'] - (leader_veh['x'] + leader_veh['Length'])) <= QUEUE_HEADWAY_DISTANCE):
                                q_c +=1
                                last_veh = leader_veh = veh
                                queued_vehicles.append(int(veh['vehid']))

                            #if first vehicle out of queue
                            elif q_c > 0:
                                q_len = last_veh['x'] - first_veh['x'] + last_veh['Length']
                                unknown_queue_output.append([str(tp), roadway, str(lane_group), str(q_len), str(q_c)])
                                max_queues = set_max_queue(q_c, q_len, roadway, first_veh['Link'], lane_group, first_veh['Link_x'], max_queues, queued_vehicles)

                                q_c = 0
                                first_veh = None
                                last_veh = None
                                leader_veh = veh
                                q_counter += 1
                                queued_vehicles = []

                                # Check the vehicle in case the vehicle was motionless but too far away from the previous queue to be added (so start a new queue)
                                if (leader_veh['v'] == QUEUE_START_SPEED):
                                      q_c = 1
                                      first_veh = last_veh = leader_veh
                                      q_len = leader_veh['Length']
                                      queued_vehicles.append(leader_veh['vehid'])


                            if len(roadway_veh) == veh_count: # This is the last vehicle on the roadway segment, print queue count and length
                                if q_c > 0:
                                    q_len = last_veh['x'] - first_veh['x'] + last_veh['Length']
                                    unknown_queue_output.append([str(tp), roadway, str(lane_group), str(q_len), str(q_c)])
                                    max_queues = set_max_queue(q_c, q_len, roadway, first_veh['Link'], lane_group, first_veh['Link_x'], max_queues, queued_vehicles)
                                else:
                                    unknown_queue_output.append([str(tp), roadway, str(lane_group), str(q_len), str(q_c)])
                                    max_queues = set_max_queue(q_c, q_len, roadway, 0, lane_group, 0, max_queues, queued_vehicles)
                    
            # Unknown Queue Step #7: Store max queue lengths over the previous time interval (max queue length for tp=120 sec is the max queue length from 61-120 seconds)
            if tp % SECONDS_INTERVAL == 0:
                for bottleneck_loc in bnlinks:
                        queued_vehicles = str(max_queues[bottleneck_loc]['queued_vehicles'])
                        q_len = str(max_queues[bottleneck_loc]['max_queue_length'])
                        q_count = str(max_queues[bottleneck_loc]['queue_count'])
                        current_datetime = start_time + datetime.timedelta(seconds = int(tp))
                        current_time = current_datetime.time()

                        max_output.append([str(current_time), bottleneck_loc,  q_len, q_count])
                        max_queues[bottleneck_loc] = {'link' : 0, 'stop_position': 0.0, 'queue_count' : 0, 'max_queue_length' : 0.0, 'queued_vehicles': [] }

            if 0 == (tp_count % 10000 ) and len(unknown_queue_output) > 0:
                write_output(out_f, unknown_queue_output)
                unknown_queue_output = []
                with open(max_queue_output_name, 'a') as max_out_f:
                    write_output(max_out_f, max_output)
                    max_output = []


        #clear queue_output before closing.
        write_output(out_f, unknown_queue_output)
        with open(max_queue_output_name, 'a') as max_out_f:
            write_output(max_out_f, max_output)

            
def set_max_queue(q_count, q_length, bottleneck, link, lane_group, stop_pos, max_queues, queued_vehicles):
    if float(max_queues[bottleneck]['max_queue_length']) < q_length:
        max_queues[bottleneck]['max_queue_length'] = q_length
        max_queues[bottleneck]['queue_count'] = q_count
        max_queues[bottleneck]['stop_position'] = stop_pos
        max_queues[bottleneck]['queued_vehicles'] = queued_vehicles

    return max_queues

def write_output(out_f, output):
  if len(output) > 0:
      print 'Writing to file'
      for line in output:
           out_f.write(','.join(line) + '\n')

def distance_between(origin_x, origin_y, destination_x, destination_y):
    distance = ((origin_x - destination_x)**2 + (origin_y - destination_y)**2)**.5
    return distance

def get_roadway_vehicles(roadway, lane_group, bnlinks, veh_list):

    roadway_veh = []
    

    for link_position, link_data in enumerate(bnlinks[roadway][lane_group]['link_list']):
        link_num, link_lane = link_data[0], link_data[1]

        for veh in veh_list:
            if (veh['Link'] == link_num):

                #change distance from end of link to distance to stop point
                veh['x'] = distance_between(bnlinks[roadway][lane_group]['link_x'], bnlinks[roadway][lane_group]['link_y'], veh['World_x'], veh['World_y'])
                roadway_veh.append(veh)

    return roadway_veh

def read_superlinks_file(filename):
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


        for col in range(6,len(row),3):
            if row[col] == '':
                break
            superlinks[row[0]][row[1]]['link_list'].append( (int(row[col]), int(row[col+1]))  ) # list in the order: link, lane, 

    # link_dict = all_links(superlinks)

    return superlinks#, link_dict

t.start('main')

parser = argparse.ArgumentParser(description='GT program for reading in fzp files and producing Unknown Queue values')
parser.add_argument('trj_filename', help = 'FZP file of vehicle trajectories') 
parser.add_argument('link_filename', help = 'CSV file of super links') 
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

trj = Trajectories(filename=args.trj_filename)

# bnlinks, keylinks = read_link_file(args.link_filename)
bnlinks = read_superlinks_file(args.link_filename)

if args.out:
    out_file = dir_path + '/' + args.out
    out_file_max = dir_path + '/' + 'max_' + args.out
else:
    out_file = dir_path + '/unknown_queue_gt_highdemandinc.csv'
    out_file_max = dir_path + '/max_unknown_queues_gt_highdemandinc.csv'

run_gt_unknown_queuing_new(trj, bnlinks, out_file, out_file_max)

t.stop('main')
print t['main']