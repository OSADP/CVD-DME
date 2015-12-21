#standard
import argparse
import datetime
import os

os.environ['TMPDIR'] = '/var/tmp'

activate_this = '/gluster/gluster1/bsm_data_emulator/bsmde_python/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

#local
from GTFileReader import Trajectories
from TCACore import Timer



QUEUE_START_SPEED = 0.0 
QUEUE_FOLLOWING_SPEED = 10.0 # ft/sec
QUEUE_HEADWAY_DISTANCE = 20.0 #ft
QUEUE_DISTANCE_WITHIN_STOP_POINT = 20 #ft 
START_HOUR = 17
SECONDS_INTERVAL = 120
CONVERT_METERS_TO_FT = 1 
t = Timer(enabled=True)




def run_gt_queuing_new(trj, bnlinks, queue_output_name, max_queue_output_name):


    queue_output = []


    # For Step #9: Print max queues for each minute
    max_output = []
    max_queues = {}
    start_time = datetime.datetime(100,1,1,int(START_HOUR),0,0)

    for bottleneck_loc in bnlinks:
        max_queues[bottleneck_loc] = {}
        for lane_group in bnlinks[bottleneck_loc].keys():
            max_queues[bottleneck_loc][lane_group] = {'link' : '', 'lane' : '', 'queue_count' : 0, 'max_queue_length' : 0.0, 'queued_vehicles': [], 'time' : 0 }

    with open(queue_output_name, 'wb') as out_f:
        out_f.write('time,bottleneck,lane_group,Queue_count,Queue_length\n')

    with open(max_queue_output_name, 'wb') as out_f:
        out_f.write('time,bottleneck,lane_group, max_queue_length,max_queue_count, time_of_queue\n')

    with open(queue_output_name, 'a') as out_f:

        tp_count  = 0

        # Known Queue Step #2: For each time step, get the list of active vehicles
        for tp, veh_list in trj.read():

            tp_count +=1

            if tp_count % 1000 ==0:
                print tp

            # Known Queue Step #3 : Loop through each bottleneck location and lane
            for bottleneck_loc in bnlinks:

                for lane_group in bnlinks[bottleneck_loc].keys():

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
                        veh_count = 0
                        queued_vehicles = []

                        # Known Queue Step #6: Starting with the vehicle closest to the end of the link
                        for veh in roadway_veh:
                            veh_count += 1
                            if q_c == 0:
                                # Known Queue Step #7: Look for a motionless vehicle within range of stop bar
                                if (veh['v'] == QUEUE_START_SPEED) and  (veh['stopbar_x'] <= QUEUE_DISTANCE_WITHIN_STOP_POINT):
                                    q_c = 1
                                    first_veh = last_veh = leader_veh = veh
                                    q_len = veh['stopbar_x'] + veh['Length']
                                    queued_vehicles.append(veh['vehid'])

                            # # Cycle through remaining vehicles on the link, determining if they are in queue behind motionless vehicle 
                            # elif (veh['v'] == QUEUE_START_SPEED) and ((veh['x'] - (leader_veh['x'] + leader_veh['Length'])) <= QUEUE_HEADWAY_DISTANCE):
                            #     q_c += 1
                            #     last_veh = leader_veh = veh
                            #     queued_vehicles.append(int(veh['vehid']))

                            elif leader_veh != None and (veh['v'] <= QUEUE_FOLLOWING_SPEED) and \
                              (( veh['x'] - ( leader_veh['x'] + leader_veh['Length'])) <= QUEUE_HEADWAY_DISTANCE):
                                q_c +=1
                                last_veh = leader_veh = veh
                                queued_vehicles.append(veh['vehid'])

                            #if first vehicle out of queue
                            elif q_c > 0:
                                q_len = last_veh['stopbar_x'] + last_veh['Length']
                                queue_output.append(([str(tp), bottleneck_loc, str(lane_group), str(q_c), str(q_len) ]))
                                max_queues = set_max_queue(q_c, q_len, bottleneck_loc, lane_group, last_veh['Link'], last_veh['Lane'], max_queues, queued_vehicles, tp)
                                break

                            if len(roadway_veh) == veh_count: # This is the last vehicle on the roadway segment, print queue count and length
                                if q_c > 0:
                                    q_len = last_veh['stopbar_x'] + last_veh['Length']
                                    last_link = last_veh['Link']
                                    last_lane = last_veh['Lane']
                                else:
                                    last_link = last_lane = '0'
                                queue_output.append(([str(tp), bottleneck_loc, str(lane_group), str(q_c), str(q_len) ]))
                                max_queues = set_max_queue(q_c, q_len, bottleneck_loc, lane_group, last_link, last_lane, max_queues, queued_vehicles, tp)

            # Known Queue Step #9: Store max queue lengths over the previous user-defined time interval 
            # (e.g. max queue length for tp=120 sec with 60 second time interval is the max queue length from 61-120 seconds)
            if tp % SECONDS_INTERVAL == 0:
                for bottleneck_loc in bnlinks:
                    for lane_group in bnlinks[bottleneck_loc].keys():
                        queued_vehicles = str(max_queues[bottleneck_loc][lane_group]['queued_vehicles'])
                        q_len = str(max_queues[bottleneck_loc][lane_group]['max_queue_length'])
                        q_count = str(max_queues[bottleneck_loc][lane_group]['queue_count'])
                        link = str(max_queues[bottleneck_loc][lane_group]['link'])
                        lane = str(max_queues[bottleneck_loc][lane_group]['lane'])
                        time = str(max_queues[bottleneck_loc][lane_group]['time'])
                        current_datetime = start_time + datetime.timedelta(seconds = int(tp))
                        current_time = current_datetime.time()

                        max_output.append([str(current_time), bottleneck_loc, lane_group, q_len, q_count, time])
                        max_queues[bottleneck_loc][lane_group] = {'link' : '', 'lane' : '', 'queue_count' : 0, 'max_queue_length' : 0.0, 'queued_vehicles': [], 'time': 0 }


            if 0 == (tp_count % 10000 ) and len(queue_output) > 0:
                write_output(out_f, queue_output)
                queue_output = []
                with open(max_queue_output_name, 'a') as max_out_f:
                    write_output(max_out_f, max_output)
                    max_output = []

        #clear queue_output before closing.
        write_output(out_f, queue_output)
        with open(max_queue_output_name, 'a') as max_out_f:
            write_output(max_out_f, max_output)
            
def set_max_queue(q_count, q_length, bottleneck, lane_group, last_link, last_lane, max_queues, queued_vehicles, tp):
    if float(max_queues[bottleneck][lane_group]['max_queue_length']) < q_length:
        max_queues[bottleneck][lane_group]['max_queue_length'] = q_length
        max_queues[bottleneck][lane_group]['queue_count'] = q_count
        max_queues[bottleneck][lane_group]['link'] = last_link
        max_queues[bottleneck][lane_group]['lane'] = last_lane
        max_queues[bottleneck][lane_group]['queued_vehicles'] = queued_vehicles
        max_queues[bottleneck][lane_group]['time'] = tp

    return max_queues


def write_output(out_f, output):
  if len(output) > 0:
      print 'Writing to file'
      for line in output:
           out_f.write(','.join(line) + '\n')


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

def distance_between(origin_x, origin_y, destination_x, destination_y):
    return ((origin_x - destination_x)**2 + (origin_y - destination_y)**2)**.5

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
            superlinks[row[0]][row[1]]['link_list'].append( (row[col], row[col+1])  ) # list in the order: link, lane, 

    return superlinks

t.start('main')

parser = argparse.ArgumentParser(description='GT program for reading in fzp files and producing Queue values')
parser.add_argument('trj_filename', help = 'FZP file of vehicle trajectories') 
parser.add_argument('superlink_filename', help = 'CSV file of super links') 
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

trj = Trajectories(filename=args.trj_filename)

bnlinks = read_superlinks_file(args.superlink_filename)

if args.out:
    out_file = dir_path + '/' + args.out
    out_file_max = dir_path + '/' + 'max_' + args.out
else:
    out_file = dir_path + '/queue_gt.csv'
    out_file_max = dir_path + '/max_queues_gt.csv'

run_gt_queuing_new(trj, bnlinks, out_file, out_file_max)

t.stop('main')
print t['main']