#standard
import argparse
import datetime
import os
import math


import pandas as pd

#local
from GTFileReader2 import BSMs, PDMs
from readlinks import read_link_file,read_endpoints
from TCACore import Timer



SHOCKWAVE_START_ACCELERATION = -.25 * 9.8
SHOCKWAVE_HEADWAY_DISTANCE = 50.0 #ft
QUEUE_SPEED = 10
FREE_FLOW = 109.361
AVG_VEHICLE_LENGTH = 20
link_positions = {}
t = Timer(enabled=True)


def run_gt_shockwave(trj, bnlinks, df_onRoute, unknown_queue_output_name):

    unknown_queue_output = []

    # Shockwave Step 1: For each instantaneous time period, t, in the simulation duration do steps 2-6
    shockwave_list = []
    for t, veh_list in trj.read():
        remove_list = []
        for start in shockwave_list:
            if start[1]["tp"] + 2 < t:
                if start[2] is not None:
                    shockwave_length = (start[2]['x'] - start[0]['x']) + AVG_VEHICLE_LENGTH
                    unknown_queue_output.append([str(start[0]['tp']), start[4], '1', str(start[0]['x']), str(shockwave_length), str(start[2]['tp']), str(start[2]['x']), str(start[5])])
                    remove_list.append(shockwave_list.index(start))
                else:
                    remove_list.append(shockwave_list.index(start))

        remove_list = sorted(remove_list, reverse=True)
        for index in remove_list:
            shockwave_list.pop(index)
        # Shockwave Step 2: For each roadway segment, L, in the set of all roadway segments do steps 3-6
        for roadway in bnlinks:
            # Shockwave Step 3: For each lane, l, on the roadway segment, L, do steps 4-6
            for lane_group in bnlinks[roadway].keys():
                # Shockwave #4: Identify IDs of all vehicles that are on link L at time t in lane l. Let I be the array of identified vehicle IDs.
                I = get_roadway_vehicles(roadway, lane_group, bnlinks, veh_list)

                I = sorted(I, key=lambda k: k['x'])

                for i in I:
                    start_flag = True
                    for start in shockwave_list:
                        if roadway == start[4]:
                            if (i['x']  - start[1]['x']) <= SHOCKWAVE_HEADWAY_DISTANCE and (i['x'] - start[1]['x']) > 0:
                                if start[5] == '4' and i['v'] <= QUEUE_SPEED: 
                                    start[1] = i
                                    start[2] = i
                                    start_flag = False 
                                elif  start[5] == '3' and ((QUEUE_SPEED < i['v'] <= (1/3) * FREE_FLOW and (i['a'] < 0))):
                                    start[1] = i
                                    start[2] = i
                                    start_flag = False
                                elif  (start[5] == '2' or start[5] == '1') and((i['v'] > (1/3) * FREE_FLOW and (i['a'] < 0))):
                                    start[1] = i
                                    start[2] = i
                                    start_flag = False
                    if start_flag:
                        if i['v'] <= QUEUE_SPEED:
                            i_s = i # ID of first vehicle in shockwave
                            i_e = None # ID of last vehicle in shockwave
                            i_f = i # ID of follower vehicle 
                            i_n = None
                            shockwave_list.append([i_s, i_f, i_e, i_n, roadway, '4'])

                        elif (QUEUE_SPEED < i['v'] <= (1/3) * FREE_FLOW and (i['a'] <= SHOCKWAVE_START_ACCELERATION)): 
                            i_s = i # ID of first vehicle in shockwave
                            i_e = None # ID of last vehicle in shockwave
                            i_f = i # ID of follower vehicle 
                            i_n = None
                            shockwave_list.append([i_s, i_f, i_e, i_n, roadway, '3'])

                        elif (i['v'] > (1/3) * FREE_FLOW and (i['a'] <= SHOCKWAVE_START_ACCELERATION)): 
                            i_s = i # ID of first vehicle in shockwave
                            i_e = None # ID of last vehicle in shockwave
                            i_f = i # ID of follower vehicle 
                            i_n = None
                            shockwave_list.append([i_s, i_f, i_e, i_n, roadway, '2'])

                        elif (i['v'] > (1/3) * FREE_FLOW and (i['a'] < 0)): 
                            i_s = i # ID of first vehicle in shockwave
                            i_e = None # ID of last vehicle in shockwave
                            i_f = i # ID of follower vehicle 
                            i_n = None
                            shockwave_list.append([i_s, i_f, i_e, i_n, roadway, '1'])

    for start in shockwave_list:
        if start[2] is not None:
            shockwave_length = (start[2]['x'] - start[0]['x']) + AVG_VEHICLE_LENGTH
            unknown_queue_output.append([str(start[0]['tp']), start[4], '1', str(start[0]['x']), str(shockwave_length), str(start[2]['tp']), str(start[2]['x']), str(start[5])])           
         
    unknown_queue_output = sorted(unknown_queue_output, key=lambda k: float(k[0]))
    print "Consolidating"
    consolidated_output = []                        
    for shockwave in unknown_queue_output:
        unconsolidated_flag = True
        for i in xrange(len(consolidated_output)):
            if shockwave[1] == consolidated_output[i][1] \
            and shockwave[7] == consolidated_output[i][7] \
            and float(shockwave[0]) - 30 <= float(consolidated_output[i][0]) \
            and (float(shockwave[3]) >= float(consolidated_output[i][3]) and float(shockwave[3]) <= float(consolidated_output[i][6]) \
            and float(shockwave[6]) >= float(consolidated_output[i][3]) and float(shockwave[6]) <= float(consolidated_output[i][6])):
                unconsolidated_flag = False
                break

        if unconsolidated_flag:
            consolidated_output.append(shockwave)

    Q = 2200.0
    J = 0.1
    T = 1.0
    FREE_FLOW_KM = 120
    for shockwave in consolidated_output:
        downstream_speed = get_downstream_speed(df_onRoute,shockwave[3], shockwave[0], shockwave[1]) 
        upstream_speed = get_upstream_speed(df_onRoute,shockwave[6], shockwave[5], shockwave[1])
        if upstream_speed != None and downstream_speed != None:
            downstream_speed = downstream_speed * 1.60934
            upstream_speed = upstream_speed * 1.60934
            if downstream_speed == 0:
                downstream_speed = .001
            if upstream_speed == 0:
                upstream_speed = .001
            downstream_q = Q * (1 + ((((1/downstream_speed) - (1/FREE_FLOW_KM))**2 - ((.25**2 * 8 * J * T)/Q))/(.25 * 2 * T * ((1/downstream_speed) - (1/FREE_FLOW_KM)))))
            upstream_q = Q * (1 + ((((1/upstream_speed) - (1/FREE_FLOW_KM))**2 - ((.25**2 * 8 * J * T)/Q))/(.25 * 2 * T * ((1/upstream_speed) - (1/FREE_FLOW_KM)))))
            try:
	    	propogation = (downstream_q - upstream_q)/((downstream_q/downstream_speed) - (upstream_q/upstream_speed))
	    except ZeroDivisionError:
		propogation = 0
        else:
            propogation = 'NA'
        if propogation != 'NA':
            propogation = propogation * 0.621371
        shockwave.append(str(propogation))
        shockwave_count = float(shockwave[4])/20
        shockwave.append(str(int(shockwave_count)))
        if shockwave_count >= 5 and shockwave[7] == '4':
            shockwave.append('Y')
        else:
            shockwave.append('N')

    consolidated_output = sorted(consolidated_output, key=lambda k: float(k[0]))
    with open(unknown_queue_output_name + "_consolidated_output.csv", "wb") as out_f:
        out_f.write('time,link,lane_group,start_location_x, shockwave_length, end_time, end_x, shockwave_type, shockwave_propogation_speed, shockwave_count, significant_shockwave\n')
        write_output(out_f, consolidated_output) 

def write_output(out_f, output):
  if len(output) > 0:
      print 'Writing to file'
      for line in output:
           out_f.write(','.join(line) + '\n')

def distance_between(origin_x, origin_y, destination_x, destination_y):
    distance = ((origin_x - destination_x)**2 + (origin_y - destination_y)**2)**.5
    return distance

def getLinkPosition(link):
    global link_positions
    for l in link_positions.keys():
        if link == l:
            x = link_positions[link]["x"]
            y = link_positions[link]["y"]
            return x,y

def get_downstream_speed(df_onRoute, start_x, start_time, roadway, distance = 50, time = 0):
    messages = []
    df_time = df_bsms[abs(df_bsms['localtime'] - float(start_time)) <= time]
    df_dist = df_time[(float(start_x) - df_time[roadway+'x'] > 0) & (float(start_x) - df_time[roadway+'x'] <= distance)]
    messages = list(df_dist.itertuples())
    total_speed = 0
    n = 0
    for message in messages:
        total_speed += message[3]
        n += 1
    if n > 0:
        average_speed = total_speed/n
        return average_speed
    if distance > 1000:
        return None

    if time == 0:
        df_time = df_bsms[abs(df_bsms['localtime'] - float(start_time)) <= time]
        df_dist = df_time[(float(start_x) - df_time[roadway+'x'] > 0) & (float(start_x) - df_time[roadway+'x'] <= distance)]
        messages = list(df_dist.itertuples())
        total_speed = 0
        n = 0
        for message in messages:
            total_speed += message[3]
            n += 1
        if n > 0:
            average_speed = total_speed/n
            return average_speed

    return get_downstream_speed(df_onRoute, start_x, start_time, roadway, distance + 50, time + 2)

def get_upstream_speed(df_onRoute, end_x, end_time, roadway, distance = 50, time = 0):
    messages = []
    df_time = df_bsms[abs(df_bsms['localtime'] - float(end_time)) <= time]
    df_dist = df_time[(df_time[roadway+'x'] - float(end_x) > 0)  & (df_time[roadway+'x'] - float(end_x) <= distance)]
    messages = list(df_dist.itertuples())
    total_speed = 0
    n = 0
    for message in messages:
        total_speed += message[3]
        n += 1
    if n > 0:
        average_speed = total_speed/n
        return average_speed
    if distance > 1000:
        return None

    if time == 0:
        df_time = df_bsms[abs(df_bsms['localtime'] - float(end_time)) <= time]
        df_dist = df_time[(df_time[roadway+'x'] - float(end_x) > 0)  & (df_time[roadway+'x'] - float(end_x) <= distance)]
        messages = list(df_dist.itertuples())
        total_speed = 0
        n = 0
        for message in messages:
            total_speed += message[3]
            n += 1
        if n > 0:
            average_speed = total_speed/n
            return average_speed
    return get_upstream_speed(df_onRoute, end_x, end_time, roadway, distance + 50, time + 2)

def get_roadway_vehicles(roadway, lane_group,bnlinks, veh_list):

    I = []
    

    for link_position, link_data in enumerate(bnlinks[roadway][lane_group]):
        link_num, link_lane, link_len = link_data[0], link_data[1], link_data[2]
        dis_stop = 0

        # If not the first link in the roadway, find the summed distance of all prior links
        if link_position != 0:
            for i in reversed(range(link_position)):
                dis_stop += bnlinks[roadway][lane_group][i][2]


        for veh in veh_list:
            if (veh['Link'] == link_num):

                #change distance from end of link to distance to stop point
                link_x, link_y = getLinkPosition(link_num)
                veh['x'] = distance_between(link_x, link_y,veh['World_x'],veh['World_y']) + dis_stop
                I.append(veh)

    return I

def dis_stop(bnlinks, roadway, lane_group):
    link_distance = {}
    for link_position, link_data in enumerate(bnlinks[roadway][lane_group]):
        link_num, link_lane, link_len = link_data[0], link_data[1], link_data[2]
        dis_stop = 0

        # If not the first link in the roadway, find the summed distance of all prior links
        if link_position != 0:
            for i in reversed(range(link_position)):
                dis_stop += bnlinks[roadway][lane_group][i][2]
        link_distance[link_num] = dis_stop

    return link_distance



t.start('main')

parser = argparse.ArgumentParser(description='GT program for reading in fzp files and producing Shockwave values')
parser.add_argument('bsm_filename') # CSV file of Basic Safety Messages
parser.add_argument('link_filename', help = 'CSV file of super links')
parser.add_argument('link_positions_filename') # CSV file of link end points 
parser.add_argument('BSM_or_PDM') # CSV file of link end points 
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

if args.BSM_or_PDM == 'BSM':
    trj = BSMs(filename=args.bsm_filename)
    df_bsms = pd.read_csv(filepath_or_buffer = args.bsm_filename, header = 0, skipinitialspace = True, usecols = ['localtime', 'spd', 'x', 'y', 'link'])
    df_bsms = df_bsms.rename(columns={'spd': 'Speed'})
else:
    trj = PDMs(filename=args.bsm_filename)
    df_bsms = pd.read_csv(filepath_or_buffer=args.bsm_filename,header=0,skipinitialspace = True,usecols=['Time_Taken','Speed','X','Y','Link'])
    df_bsms = df_bsms.rename(columns={'Link': 'link'})
    df_bsms = df_bsms.rename(columns={'X': 'x'})
    df_bsms = df_bsms.rename(columns={'Y': 'y'})
    df_bsms = df_bsms.rename(columns={'Time_Taken': 'localtime'})

bnlinks, keylinks = read_link_file(args.link_filename)

link_positions = read_endpoints(args.link_positions_filename)

df_bsms = df_bsms[(df_bsms['localtime'] >= 2700.0) & (df_bsms['localtime'] <= 4500.0)]

roadway_links = []
for roadway in bnlinks:
    for lane_group in bnlinks[roadway]:
        for link_data in bnlinks[roadway][lane_group]:
            link_num, link_lane, link_len = link_data[0], link_data[1], link_data[2]
            roadway_links.append(link_num)

df_bsms = df_bsms[df_bsms.link.isin(roadway_links)]

for roadway in bnlinks:
    for lane_group in bnlinks[roadway]:
        link_distance = dis_stop(bnlinks, roadway, lane_group)
        df_bsms[roadway+'x'] = df_bsms.apply(lambda row: link_distance[float(row['link'])] + distance_between(getLinkPosition(float(row['link']))[0], getLinkPosition(float(row['link']))[1], row['x'], row['y']), axis = 1)

print "roadwayx set"
if args.out:
    out_file = dir_path + '/' + args.out

else:
    out_file = dir_path + '/pdm408_shockwaves.csv'

run_gt_shockwave(trj, bnlinks, df_bsms, out_file)

t.stop('main')
print t['main']