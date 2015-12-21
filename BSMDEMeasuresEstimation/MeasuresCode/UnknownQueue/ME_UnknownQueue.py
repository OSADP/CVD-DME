#standard
import argparse
import datetime
import os
os.environ['TMPDIR'] = '/var/tmp'

activate_this = '/gluster/gluster1/bsm_data_emulator/bsmde_python/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

#local
from MEFileReader import Messages
from TCACore import Timer, Chk_Range

BSM_100percent = False
QUEUE_START_SPEED = 0.0
QUEUE_FOLLOWING_SPEED = 10.0 #ft/s 
QUEUE_FOLLOWING_DISTANCE_100PERCENT = 20 # ft
QUEUE_FOLLOWING_DISTANCE = 50 #ft
QUEUE_SPEED = 5.0 #ft/s
AVG_VEHICLE_LENGTH = 20
START_HOUR = 17
CONVERT_METERS_TO_FT = 100 / 2.54 / 12 
SECONDS_INTERVAL = 120
t = Timer(enabled=True)

def run_ME_unknown_queue(msgs, superlinks, link_dict, all_queues_output_name, max_queue_output_name):

    queue_output = []
    max_output = []
    active_vehicles = {}
    max_queues = {}
    start_time = datetime.datetime(100,1,1,int(START_HOUR),0,0)

    for bottleneck_name in superlinks:
        max_queues[bottleneck_name] = {}
        for lane_group in superlinks[bottleneck_name]:
            max_queues[bottleneck_name][lane_group] = {'link' : 0, 'lane' : 0, 'queue_count' : 0, 'max_queue_length' : 0.0, 'current_msgs': [] }

    with open(all_queues_output_name, 'wb') as out_f:
        out_f.write('time,bottleneck_name,lane_group,queue_count,queue_length\n')

        # ME Queue Step #2: For each time period
        for tp, msg_list in msgs.read():
            # Identify all messages on identified bottleneck links, assign each message to their bottleneck and lane_group
            for msg in msg_list:
                if int(msg['link']) in link_dict.keys():
                    bottleneck_name = link_dict[msg['link']]['bottleneck_name']
                    lane_group = link_dict[msg['link']]['lane_group']
                    max_queues[bottleneck_name][lane_group]['current_msgs'].append(msg)
                    if int(msg['link']) == 142:
                        print msg


            # ME Queue Step #3 and #4: For each bottleneck location and each lane_group
            for bottleneck in superlinks:
                for lane_group in superlinks[bottleneck]:
                    i_f = None
                    i_e = None
                    f_q = 0
                    f_m = 0
                    last_SS_list = []
                    l_x = superlinks[bottleneck][lane_group]['link_x']
                    l_y = superlinks[bottleneck][lane_group]['link_y']

                    # ME Queue Step #5: Retrieve all messages found on the links in the superlink, determine position xi and speed vi
                    msg_list = max_queues[bottleneck][lane_group]['current_msgs']
                    if len(msg_list) > 0:
                        max_queues[bottleneck][lane_group]['current_msgs'] = []

                        for msg in msg_list:
                            msg['linkdistance'] = distance_between(l_x, l_y, msg['x'], msg['y'])
                            # msg['distance'] = distance_between(b_x, b_y, msg['x'], msg['y'])

                        # ME Queue Step #6: Sort all BSMs by increasing distance from the bottleneck stopline
                        sorted_msg_list = sorted(msg_list, key=lambda k: k['linkdistance'])

                        if BSM_100percent:
                            q_count = 0
                            for msg in sorted_msg_list:
                                if i_e is None:
                                    if msg['v'] == QUEUE_START_SPEED:
                                        if i_f is None:
                                            i_f = msg
                                        q_count += 1  
                                        i_e = msg
                                        f_q = 1  
                                        # logger.debug("Queue started")                          
                                else:
                                    if (msg['v'] <= QUEUE_FOLLOWING_SPEED) and ((msg['linkdistance'] - (i_e['linkdistance'] + AVG_VEHICLE_LENGTH)) <= QUEUE_FOLLOWING_DISTANCE_100PERCENT): 
                                        q_count += 1
                                        i_e = msg
                                        f_q = 1
                                        # logger.debug("Added to queue")
                                    elif ((msg['linkdistance'] - (i_e['linkdistance'] + AVG_VEHICLE_LENGTH)) > QUEUE_FOLLOWING_DISTANCE_100PERCENT):
                                        q_length = f_q * (i_e['linkdistance'] - (i_f['linkdistance'] + AVG_VEHICLE_LENGTH))
                                        link = i_e['link']
                                        last_SS_list.append(i_e) # Add the last msg in queue to the end
                                        f_q = 0 
                                        f_m = 1
                                        i_e = None
                                        i_f = None

                                        max_queues = set_max_queue(q_count, q_length, bottleneck, link, lane_group, max_queues)
                                        out_f.write('%s,%s,%s,%d,%s\n' %(str(tp), bottleneck, lane_group, q_count, str(q_length)))
                            if f_q > 0:
                                q_length = f_q * (i_e['linkdistance'] - (i_f['linkdistance'] + AVG_VEHICLE_LENGTH))
                                link = i_e['link']
                                # logger.debug("Max queue length found of: %s" % q_length)
                            elif f_m == 0:
                                q_count = 0
                                q_length = 0
                                link = 'NA'

                        else:
                            # ME Queue Step #7: For each BSM, determine if the vehicle is in a queued state
                            for msg in sorted_msg_list:
                                if i_e is None:
                                    if msg['v'] <= QUEUE_SPEED:
                                        if i_f is None:
                                            i_f = msg
                                        i_e = msg
                                        f_q = 1  
                                        # logger.debug("Queue started")                          
                                else:
                                    if (msg['v'] <= QUEUE_SPEED) and ((msg['linkdistance'] - (i_e['linkdistance'] + AVG_VEHICLE_LENGTH)) <= QUEUE_FOLLOWING_DISTANCE):
                                        f_q = 1
                                        i_e = msg
                                        # logger.debug("Added to queue")
                                    elif ((msg['linkdistance'] - (i_e['linkdistance'] + AVG_VEHICLE_LENGTH)) > QUEUE_FOLLOWING_DISTANCE):
                                        q_length = f_q * (i_e['linkdistance'] - (i_f['linkdistance'] + AVG_VEHICLE_LENGTH))
                                        q_count = q_length / AVG_VEHICLE_LENGTH
                                        link = i_e['link']
                                        last_SS_list.append(i_e) # Add the last msg in queue to the end
                                        f_q = 0 
                                        f_m = 1
                                        i_e = None
                                        i_f = None

                                        max_queues = set_max_queue(q_count, q_length, bottleneck, link, lane_group, max_queues)
                                        out_f.write('%s,%s,%s,%d,%s\n' %(str(tp), bottleneck, lane_group, q_count, str(q_length)))

                            if f_q > 0:
                                q_length = f_q * (i_e['linkdistance'] - (i_f['linkdistance'] + AVG_VEHICLE_LENGTH))
                                q_count = q_length / AVG_VEHICLE_LENGTH
                                link = i_e['link']
                                # logger.debug("Max queue length found of: %s" % q_length)
                            elif f_m == 0:
                                q_count = 0
                                q_length = 0
                                link = 'NA'

                        last_SS_list.append(i_e) # Add the last msg in queue to the end
                        f_q = 0 
                        f_m = 0
                        i_e = None
                        i_f = None

                        max_queues = set_max_queue(q_count, q_length, bottleneck, link, lane_group, max_queues)
                        out_f.write('%s,%s,%s,%d,%s\n' %(str(tp), bottleneck, lane_group, q_count, str(q_length)))

            if tp % SECONDS_INTERVAL == 0:
                for bottleneck in superlinks:
                    for lane_group in superlinks[bottleneck]:
                        q_len = str(max_queues[bottleneck][lane_group]['max_queue_length'])
                        q_count = str(int(max_queues[bottleneck][lane_group]['queue_count']))
                        link = str(max_queues[bottleneck][lane_group]['link'])
                        current_datetime = start_time + datetime.timedelta(seconds = int(tp))
                        current_time = current_datetime.time()

                        max_output.append([str(current_time), bottleneck, lane_group, link, q_len, q_count])
                        max_queues[bottleneck][lane_group] = {'link' : 0, 'lane': 0, 'queue_count' : 0, 'max_queue_length' : 0.0, 'current_msgs' : [] }
    
    write_output(max_queue_output_name,max_output)


def set_max_queue(q_count, q_length, bottleneck, link, lane, max_queues):
    if float(max_queues[bottleneck][lane]['max_queue_length']) < q_length:
        max_queues[bottleneck][lane]['max_queue_length'] = q_length
        max_queues[bottleneck][lane]['queue_count'] = q_count
        max_queues[bottleneck][lane]['link'] = link
        max_queues[bottleneck][lane]['lane'] = lane

    return max_queues

def distance_between(origin_x, origin_y, destination_x, destination_y):
    return ((origin_x - destination_x)**2 + (origin_y - destination_y)**2)**.5

def write_output(filename, output):
  if len(output) > 0:
    with open(filename, 'w') as f_out:
      print 'Writing to file'
      f_out.write('Time,Bottleneck,Lane_group,Link,Max_Queue_Length,Max_Queue_Count\n')
      for line in output:
           f_out.write(','.join(line) + '\n')
  else:
    print 'No output'


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

        for col in range(6,len(row),3):
            if row[col] == '':
                break
            superlinks[row[0]][row[1]]['link_list'].append( (int(row[col]), int(row[col+1]), float(row[col+2]))  ) # list in the order: link, lane, length

    link_dict = all_links(superlinks)

    return superlinks, link_dict

def all_links(superlinks):
    link_dict = {}
    for superlink in superlinks:
        for lane_group in superlinks[superlink]:
            for link_position, link_data in enumerate(superlinks[superlink][lane_group]['link_list']):
                link_num = link_data[0]
                lane_num = link_data[1]
                if link_num not in link_dict.keys():
                    link_dict[link_num] = {}
                link_dict[link_num]['bottleneck_name'] = superlink
                link_dict[link_num]['lane_group'] = lane_group

    return link_dict


t.start('main')

parser = argparse.ArgumentParser(description='Measures Estimation program for reading in BSM and/or PDM files and producing Queue values')
parser.add_argument('BSM_filename', help = 'FZP file of vehicle trajectories') 
parser.add_argument('superlink_filename', help = 'CSV file of superlink coordinates and links')  
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

superlinks, link_dict = read_superlink_file(args.superlink_filename)

msgs = Messages(args.BSM_filename,link_dict, interpolate = False)

if args.out:
    out_file = dir_path + '/' + 'all_' + args.out
    max_out_file = dir_path + '/' + 'max_' + args.out
else:
    out_file = dir_path + '/all_unknown_queues_me.csv'
    max_out_file = dir_path + '/max_unknown_queues_me.csv'
# BSM_filename = r'C:\Users\M29565\Documents\Projects\tca\Measures_code\UnknownQueue\BSMTrans.csv'
# superlink_filename = r'C:\Users\M29565\Documents\Projects\tca\Measures_code\UnknownQueue\intersection_bottlenecks.csv'

# dir_path = os.path.dirname( os.path.realpath( __file__ ) )

# msgs = Messages(BSM_filename)

# superlinks, link_dict = read_superlink_file(superlink_filename)

# out_file = dir_path + '/max_unknown_queue_me.csv'

run_ME_unknown_queue(msgs, superlinks, link_dict, out_file, max_out_file)

t.stop('main')
print t['main']
