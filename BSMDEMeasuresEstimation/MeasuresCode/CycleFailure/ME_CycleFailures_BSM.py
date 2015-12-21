#standard
import argparse
import datetime
import os

import pandas as pd
import math
import numpy as np

#local
from TCACore import Timer, Chk_Range
from readlinks import read_link_file, read_intersection_file, read_signal_controllers_file, read_greentimes_file, read_full_routes, read_link_length, read_endpoints
from MEFileReader import Messages

DEBUG = 1
BSM_100percent = True
QUEUE_START_SPEED = 0.0
QUEUE_FOLLOWING_SPEED = 10.0 #ft/s 
QUEUE_DISTANCE_FROM_STOPBAR = 100 #ft
QUEUE_FOLLOWING_DISTANCE = 100 # ft
QUEUE_SPEED = 5.0 #ft/s
CONVERT_METERS_TO_FT = 100 / 2.54 / 12 
AVG_VEHICLE_LENGTH = 20
link_lengths = {}
link_positions = {}
full_routes = {}
MIN_BSM = 4
dir_path = os.path.dirname( os.path.realpath( __file__ ) )

def run_me_cyclefailure(msgs, superlinks, int_map, green_times, red_times, df_bsms, downstream_superlinks, output_name):
    output = []

    messages_hash = {}
    max_queues = {}
    for bottleneck in superlinks:
        messages_hash[bottleneck] = {}
        for lane_group in superlinks[bottleneck]:
          messages_hash[bottleneck][lane_group] = {}
          messages_hash[bottleneck][lane_group]['current_msgs'] = []
        max_queues[bottleneck] = {}
        for lane_group in superlinks[bottleneck]:
            max_queues[bottleneck][lane_group] = {'link' : 0, 'lane' : 0, 'max_queue_length' : 0.0, 'last_veh_greentime': None}


    with open(output_name, 'wb') as out_f:
        out_f.write('intersection,lane_group,green_starttime,next_greentime, failure_status\n')

    with open(output_name, 'a') as out_f:

        tp_count  = 0
        tp_start = True

        # read through message file time period by time period
        for tp, msg_list in msgs.read():

            # If first time period, set the current green time for each intersection lane_group to the one that matches the vehicle trajectory data best
            if tp_start:
                for intersection in int_map.keys():
                    for lane_group in int_map[intersection].keys():
                        controller_num = int_map[intersection][lane_group]['controller_num']
                        signal_grp = int_map[intersection][lane_group]['signal_group']
                        int_map[intersection][lane_group]['last_veh_greentime'] = None

                        # Loop through possible green times to find the one valid for the first tp 
                        for greentime in green_times[controller_num][signal_grp]:
                            if tp <= greentime:
                                int_map[intersection][lane_group]['current_greentime'] = greentime
                                break
                        int_map[intersection][lane_group]['current_redtime'] = red_times[controller_num][signal_grp][green_times[controller_num][signal_grp].index(int_map[intersection][lane_group]['current_greentime'])]
                tp_start = False

            # Step #2 and #3: Loop through each intersection_approach and lane_group (movement)
            for intersection_approach in int_map.keys():
                for lane_group in int_map[intersection_approach].keys():
                    controller_num = int_map[intersection_approach][lane_group]['controller_num']
                    signal_grp = int_map[intersection_approach][lane_group]['signal_group']

                    # Set the next green time to next greentimetime in the list or the last time step of the simulation
                    current_greentime = int_map[intersection_approach][lane_group]['current_greentime']
                    current_redtime = int_map[intersection_approach][lane_group]['current_redtime']
                    num_greentimes = len(green_times[controller_num][signal_grp])

                    # There are no more valid greentimes
                    if current_greentime == LAST_TIME_STEP or current_redtime == LAST_TIME_STEP:
                        continue

                    # Check each intersection_approach,lane_group green time to see if a new green time has started
                    # (PDMs) Step #5 If a new cycle (greentime) is started, determine if any of the PSNs from the previous start of the green time are still in queue (cycle failure)
                    if tp >= current_redtime:
                        route_links = []
                        for link in full_routes[intersection_approach][lane_group]:
                            route_links.append(link)
                        route_links.append(downstream_superlinks[intersection_approach][lane_group])
                        df_onRoute = df_bsms[df_bsms.link.isin(route_links)]
                        check_failure(int_map[intersection_approach][lane_group]['last_veh_greentime'], intersection_approach, lane_group, df_onRoute, current_redtime, intersection_approach, lane_group,int_map, tp, out_f)
                                # Find the cycle start time for that intersection to include in the output
                      # Reset active cycle ids and current greentime start time
                        if green_times[controller_num][signal_grp].index(current_greentime) + 1 >= num_greentimes:
                            next_greentime = LAST_TIME_STEP
                        else:
                            next_greentime = green_times[controller_num][signal_grp][green_times[controller_num][signal_grp].index(current_greentime)+1]
                        int_map[intersection_approach][lane_group]['current_greentime'] = next_greentime
                        int_map[intersection_approach][lane_group]['last_veh_greentime'] = None
                        int_map[intersection_approach][lane_group]['current_greentime_ids'] = []
                        try:
                            int_map[intersection_approach][lane_group]['current_redtime'] = red_times[controller_num][signal_grp][green_times[controller_num][signal_grp].index(next_greentime)]
                        except:
                            int_map[intersection_approach][lane_group]['current_redtime'] = LAST_TIME_STEP
                            print "No red time found", controller_num, signal_grp, int_map[intersection_approach][lane_group]['current_greentime'], green_times[controller_num][signal_grp].index(current_greentime)
                            out_f.write("%s,%s,%.1f,%.1f,No failure-Uncertain\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],tp))



            tp_count +=1

            if tp_count % 1000 ==0:
                print tp


            # Identify all messages on identified bottleneck links, assign each message to their bottleneck and lane_group
            for msg in msg_list:
                if int(msg['link']) in link_dict.keys():
                    bottleneck_name = link_dict[msg['link']]['bottleneck_name']
                    lane_group = link_dict[msg['link']]['lane_group']
                    messages_hash[bottleneck_name][lane_group]['current_msgs'].append(msg)


                       # ME Queue Step #3 and #4: For each bottleneck location and each lane_group
            for bottleneck in superlinks:
                for lane_group in superlinks[bottleneck]:
                    # logger.debug("Processing bottleneck: %s and lane_group: %s" % (bottleneck, lane_group))

                    if tp > int_map[bottleneck][lane_group]['current_greentime']:
                        messages_hash[bottleneck][lane_group]['current_msgs'] = []
                        continue
                    
                    b_x = superlinks[bottleneck][lane_group]['stopbar_x']
                    b_y = superlinks[bottleneck][lane_group]['stopbar_y']
                    l_x = superlinks[bottleneck][lane_group]['link_x']
                    l_y = superlinks[bottleneck][lane_group]['link_y']

                    # ME Queue Step #5: Retrieve all messages found on the links in the superlink, determine position xi and speed vi
                    msg_list = messages_hash[bottleneck][lane_group]['current_msgs']
                    # logger.debug("Number of messages is: %d" % len(msg_list))

                    if len(msg_list) > 0:
                        messages_hash[bottleneck][lane_group]['current_msgs'] = []
                        # msg_list['distance'] = ((b_x - msg_list['x'])**2 + (b_y - msg_list['y'])**2)**.5 

                        for msg in msg_list:
                            # Set the distance of the message from the stopbar
                            msg['linkdistance'] = distance_between(l_x, l_y, msg['x'], msg['y'])
                            msg['distance'] = distance_between(b_x, b_y, msg['x'], msg['y'])

                        # ME Queue Step #6: Sort all BSMs by increasing distance from the bottleneck stopline
                        sorted_msg_list = sorted(msg_list, key=lambda k: k['linkdistance'])

                        # Determine the vehicles in queue at green time starts
                        i_e = None
                        f_q = 0
                        last_SS_list = []

                        # ME Queue Step #7: For each BSM, determine if the vehicle is in a queued state
                        if BSM_100percent:
                            for msg in sorted_msg_list:
                                if i_e is None:
                                    if msg['v'] == QUEUE_START_SPEED and msg['distance'] <= QUEUE_DISTANCE_FROM_STOPBAR:
                                        i_e = msg
                                        f_q = 1  
                                        # logger.debug("Queue started")                          
                                else:
                                    if (msg['v'] <= QUEUE_FOLLOWING_SPEED) and ((msg['linkdistance'] - (i_e['linkdistance'] + AVG_VEHICLE_LENGTH)) <= QUEUE_FOLLOWING_DISTANCE): 
                                        i_e = msg
                                        f_q = 1
                                        # logger.debug("Added to queue")
                                    else:
                                        # logger.debug("Queue ended")
                                        break
                            if f_q > 0:
                                q_length = i_e['distance']
                                max_queues = set_max_queue(q_length, bottleneck, lane_group, i_e, max_queues)

                            if tp == int_map[bottleneck][lane_group]['current_greentime']:
                                int_map[bottleneck][lane_group]['last_veh_greentime'] = max_queues[bottleneck][lane_group]['last_veh_greentime']
                                #logger.debug("For %s %s last vehicle time is %.1f" % (bottleneck, lane_group, i_e['tp']))

                        else:
                            for msg in sorted_msg_list:
                                # logger.debug("Message distance from stopbar is: %s and speed is: %s ft/sec" % (msg['distance'],msg['v']))
                                if i_e is None:
                                    if msg['v'] <= QUEUE_SPEED and msg['distance'] <= QUEUE_DISTANCE_FROM_STOPBAR:  
                                        i_e = msg
                                        f_q = 1  
                                        # logger.debug("Queue started")                          
                                else:
                                    if (msg['v'] <= QUEUE_SPEED) and ((msg['linkdistance'] - (i_e['linkdistance'] + AVG_VEHICLE_LENGTH)) <= QUEUE_FOLLOWING_DISTANCE): 
                                        i_e = msg
                                        f_q = 1
                                        # logger.debug("Added to queue")

                            if f_q > 0:
                                q_length = i_e['distance']
                                max_queues = set_max_queue(q_length, bottleneck, lane_group, i_e, max_queues)

                            if tp == int_map[bottleneck][lane_group]['current_greentime']:
                                int_map[bottleneck][lane_group]['last_veh_greentime'] = max_queues[bottleneck][lane_group]['last_veh_greentime']
                                max_queues[bottleneck][lane_group] = {'link' : 0, 'lane' : 0, 'max_queue_length' : 0.0, 'last_veh_greentime': None}


def set_max_queue(q_length, bottleneck, lane_group, last_veh, max_queues):
    if float(max_queues[bottleneck][lane_group]['max_queue_length']) < float(q_length):
        max_queues[bottleneck][lane_group]['max_queue_length'] = q_length
        max_queues[bottleneck][lane_group]['link'] = bottleneck
        max_queues[bottleneck][lane_group]['lane'] = lane_group
        max_queues[bottleneck][lane_group]['last_veh_greentime'] = last_veh

    return max_queues

def check_failure(vehicle, route_group, route, df_onRoute, red_time, intersection_approach, lane_group, int_map, tp, out_f):
    if vehicle is None:
        print "No vehicle", route_group, red_time
        out_f.write("%s,%s,%.1f,%.1f,No queue found\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],tp))
        return
    vehicle['end_time'] = None
    link_x, link_y = getLinkPosition(vehicle['link'])
    vehicle['dist_traveled'] = get_link_length(vehicle['link']) - distance_between(link_x, link_y, vehicle['x'], vehicle['y'])
    start_time = vehicle["tp"]
    while vehicle["end_time"] is None:
        messages = get_bsms(vehicle, df_onRoute, 5, 20, route_group, route)
        message_weights = []
        if messages is None:
            print "Output not completed ", vehicle 
            break
        for message in messages:
            time_diff = (vehicle["tp"] - message[2])**2
            if message[3] != 0.0:
                dis_diff = (distance_between(vehicle["x"], vehicle["y"], message[4], message[5])/message[3])**2
            else:
                speed = 0.0001
                dis_diff = (distance_between(vehicle["x"], vehicle["y"], message[4], message[5])/speed)**2
            message_weights.append([message, 1/(time_diff + dis_diff)**.5])
        sorted(message_weights, reverse=True, key=getKey)
        new_speed_numerator = 0
        new_speed_denominator = 0
        for i in range (0,min(len(message_weights),8)):
            new_speed_numerator += message_weights[i][1] * message_weights[i][0][3]
            new_speed_denominator += message_weights[i][1]
        new_speed = new_speed_numerator/new_speed_denominator
        distance_to_travel = new_speed 
        vehicle["tp"] += 1
        move_vehicle(vehicle, new_speed, distance_to_travel, vehicle["link"], route_group, route)
    if vehicle["end_time"] is not None:
        x = -.15
        s = .3
        e = np.random.normal(x, s)
        duration = vehicle["end_time"] - start_time
        ModTT = vehicle["end_time"] + (e * vehicle["end_time"])
        if ModTT < red_time:
            if ModTT == 0:
                out_f.write("%s,%s,%.1f,%.1f,No failure-Certain\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],tp))
                return
            elif vehicle["end_time"] + (duration + ((x + (1.96 * s)) * duration)) < red_time:
                out_f.write("%s,%s,%.1f,%.1f,No failure-Certain\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],tp))
                return
            else:
                out_f.write("%s,%s,%.1f,%.1f,No failure-Uncertain\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],tp))
                return
                #Uncertain
        else:
            if vehicle["end_time"] + (duration + ((x + (1.96 * s)) * duration)) >= red_time:
                out_f.write("%s,%s,%.1f,%.1f,Failure-Certain\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],tp))
                return
            else:
                out_f.write("%s,%s,%.1f,%.1f,Failure-Uncertain\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],tp))
                return
                #Uncertain 
    print "Error, no travel time found", route_group, route, vehicle["end_time"], red_time
    out_f.write("%s,%s,%.1f,%.1f, Uncertain\n" % (intersection_approach,lane_group,int_map[intersection_approach][lane_group]['current_greentime'],tp))
    return

def getKey(item):
    return item[1]

def get_bsms(vehicle, df_bsms, time_max, distance_max, route_group, route):
    messages = []
    df_time = df_bsms[(abs(vehicle["tp"] - df_bsms["localtime"]) <= time_max)]
    df_time = df_time[(distance_between(vehicle["x"], vehicle["y"], df_time['x'], df_time['y']) <= distance_max)]
    messages = list(df_time.itertuples())
    if len(messages) >= MIN_BSM:
        return messages
    # Maximum search time to prevent an infinte loop in the case that no messages are found
    elif time_max > 1200: 
        return None
    else:
        return get_bsms(vehicle, df_bsms, time_max + 5, distance_max + 20, route_group, route)

def move_vehicle(vehicle, new_speed, distance, link, route_group, route):
    link_len = get_link_length(link)
    if link_len < 0:
        link_len = 0
    link_x, link_y = getLinkPosition(link)
    if vehicle["dist_traveled"] + distance >= link_len:
        new_distance = (vehicle["dist_traveled"] + distance) - link_len
        old_distance = link_len - vehicle["dist_traveled"]
        if old_distance > 0.0:
            vehicle["x"], vehicle["y"] = travelDistance(vehicle["x"], vehicle["y"], link_x, link_y, old_distance)
        vehicle["dist_traveled"] = 0
        link = getNextLink(link, route_group, route)
        if link is None:
            finishTrip(vehicle, new_distance, new_speed)
            return
        else:
            vehicle["link"] = link
            move_vehicle(vehicle, new_speed, new_distance, link, route_group, route)
    else:
        vehicle["dist_traveled"] += distance
        vehicle["x"], vehicle["y"] = travelDistance(vehicle["x"], vehicle["y"], link_x, link_y, distance)

def finishTrip(vehicle, distance, new_speed):
    vehicle["end_time"] = vehicle["tp"] - (distance/new_speed)

def travelDistance(origin_x, origin_y, destination_x, destination_y, distance):
    distance_between_points = ((destination_x - origin_x)**2 + (destination_y - origin_y)**2)**.5
    new_x = origin_x + ((distance/distance_between_points) * (destination_x - origin_x))
    new_y = origin_y + ((distance/distance_between_points) * (destination_y - origin_y))
    return new_x, new_y

def get_link_length(link):

    for l in link_lengths.keys():
        if link == l:
            length = link_lengths[l]
            return length

def getLinkPosition(link):

    for l in link_positions.keys():
        if link == l:
            x = link_positions[link]["x"]
            y = link_positions[link]["y"]
            return x,y

def getNextLink(link, route_group, route):

    flag = False
    for l in full_routes[route_group][route]:
        if flag:
            return l
        if l == link:
            flag = True

def distance_between(origin_x, origin_y, destination_x, destination_y):
    return ((origin_x - destination_x)**2 + (origin_y - destination_y)**2)**.5

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
            superlinks[row[0]][row[1]]['link_list'].append(int(row[col])) # list in the order: link, lane, length

    link_dict = all_links(superlinks)

    return superlinks, link_dict

def all_links(superlinks):
    link_dict = {}
    for superlink in superlinks:
        for lane_group in superlinks[superlink]:
            for link_position, link_data in enumerate(superlinks[superlink][lane_group]['link_list']):
                link_num = link_data
                if link_num not in link_dict.keys():
                    link_dict[link_num] = {}
                link_dict[link_num]['bottleneck_name'] = superlink
                link_dict[link_num]['lane_group'] = lane_group

    return link_dict

def read_downstream(filename):
    downstream_superlinks = {}
    for line in open(filename):
        row = line.strip().split(',')
        if row[0] not in downstream_superlinks.keys():
            downstream_superlinks[row[0]] = {} # Key is superlink/bottleneck name
        downstream_superlinks[row[0]][row[1]] = row[2]
    return downstream_superlinks
       
#t.start('main')

parser = argparse.ArgumentParser(description='GT program for reading in fzp files and producing Cycle Failure values')
parser.add_argument('msg_filename') #BSMs
parser.add_argument('bottleneck_filename') #vanness_bottlenecks.csv
parser.add_argument('signal_controllers_filename') # vanness_signal_controlers.csv - Map of intersection approach and lane group to signal head
parser.add_argument('greentimes_filename') #medDemand_Incident.lsa - VISSIM output 
parser.add_argument('link_positions_filename') # CSV file of link end points
parser.add_argument('length_filename') # CSV file of link lengths
parser.add_argument('fullroutes_filename') # CSV file of full routes
parser.add_argument('downstream_filename') # CSV file of link lengths
parser.add_argument('BSM_or_PDM') # CSV file of link end points  
parser.add_argument('timestep')
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

msgs = Messages(filename=args.msg_filename)

if args.BSM_or_PDM == 'BSM':
    df_bsms = pd.read_csv(filepath_or_buffer = args.msg_filename, header = 0, skipinitialspace = True, usecols = ['localtime', 'spd', 'x', 'y', 'link'])
    df_bsms = df_bsms.rename(columns={'spd': 'Speed'})
else:
    df_bsms = pd.read_csv(filepath_or_buffer=args.msg_filename,header=0,skipinitialspace = True,usecols=['Time_Taken','Speed','X','Y','Link'])
    df_bsms = df_bsms.rename(columns={'Link': 'link'})
    df_bsms = df_bsms.rename(columns={'X': 'x'})
    df_bsms = df_bsms.rename(columns={'Y': 'y'})
    df_bsms = df_bsms.rename(columns={'Time_Taken': 'localtime'})

df_bsms['Speed'] = df_bsms['Speed'].apply(lambda x: x * 1.46667)

superlinks, link_dict = read_superlink_file(args.bottleneck_filename)
int_map = read_signal_controllers_file(args.signal_controllers_filename)
green_times, red_times = read_greentimes_file(args.greentimes_filename)

full_routes = read_full_routes(args.fullroutes_filename)

link_lengths = read_link_length(args.length_filename)

link_positions = read_endpoints(args.link_positions_filename)

downstream_superlinks = read_downstream(args.downstream_filename)
LAST_TIME_STEP = args.timestep
if args.out:
    out_file = dir_path + '/' + args.out
else:
    out_file = dir_path + '/me_cyclefailure_output.csv'

run_me_cyclefailure(msgs, superlinks, int_map, green_times, red_times, df_bsms, downstream_superlinks, out_file)

#t.stop('main')
#print t['main']