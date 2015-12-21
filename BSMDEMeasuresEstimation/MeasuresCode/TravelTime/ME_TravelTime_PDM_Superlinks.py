#standard
import argparse
import datetime
import os

import pandas as pd
import math

#local
from TCACore import Timer, Chk_Range, logger
from readlinks import read_routes, read_link_length, read_full_routes, read_endpoints

t = Timer(enabled=False)
CONVERT_MPH_TO_FPS = 5280 / 60 / 60
DEBUG = 0 # Set to 1 or 2 for debug output printed to the log file
START_HOUR = 17
SECONDS_INTERVAL = 300 # seconds
TIME_WINDOW = 5
DIST_WINDOW = 20
MIN_PDMS = 4 # Minimum number of PDMs needed to calculate new speed of the hypothetical vehicle
MAX_PDMS = 8 # Maximum number of PDMs used to calculate new speed
MIN_TIME = 0
downstream_superlinks = {}
invalid_tt_count = 0 # Count of how many travel times could not be calculated due to a travel time starting/ending too early/late
no_tp_due_to_PDMs = 0 #Count of how many travel times could not be calculated due to not enough PDMs found to calculate new speed

def run_ME_traveltime_PDMs(df_pdms, routes, simulation_seconds, traveltime_output_name = 'traveltime_me_pdm.csv'):
    global invalid_tt_count
    sim_start_time = datetime.datetime(100,1,1,int(START_HOUR),0,0)
    output_list = []
    
    # Create a data storage structure to store the travel times for each time period on each route
    t_times_dict = {}
    for route_group in routes:
        t_times_dict[route_group] = {}
        for route in routes[route_group]:
            t_times_dict[route_group][route] = {}
            for start_time in range(0,simulation_seconds+SECONDS_INTERVAL,SECONDS_INTERVAL):
                time_period_start = get_time_period(start_time)
                t_times_dict[route_group][route][time_period_start] = {}
                t_times_dict[route_group][route][time_period_start]['tt_list'] = []
                t_times_dict[route_group][route][time_period_start]['avg_speed'] = 'NA'


    try:
        os.remove(traveltime_output_name)
    except OSError:
        pass
            
    with open(traveltime_output_name, 'w') as out_f:
        out_f.write('PSN,Route_Group,Route,Start_Time,End_Time,Travel_Time\n')

    total_tt = 0

    df_pdms['Speed'] * CONVERT_MPH_TO_FPS #Convert speed from mph to ft/s

    # ME Travel Time Step #1: For each route
    for route_group in routes:
        PSN_count = 0

        for route in routes[route_group]:

            # ME Travel Time Step #2: Identify all unqiue PSNs and corresponding PDMs on that route
            df_pdms['onRoute'] = df_pdms.apply(lambda row: onRoute(row['Link'], route_group, route), axis = 1)
            df_route_pdms = df_pdms[df_pdms['onRoute'] == True]
            unique_PSNs = df_route_pdms.PSN.unique()
            if DEBUG > 1:
                logger.debug("Number of unique PSNs is: %d on route: %s route_group: %s" % (df_route_pdms.PSN.nunique(), route, route_group))
        
            # Travel Times Step #3: For each unique PSN on the route
            for PSN in unique_PSNs:
                PSN_count = PSN_count + 1
                if PSN_count % 100 == 0:
                    print "Number of PSNs processed: %d for route_group: %s and route: %s" % (PSN_count, route_group, route)
                vehicle = {}

                # Get all the PDMs on the route associated with PSN 
                t.start('get_PSN_PDMs_df')
                df_psn = df_route_pdms[df_pdms.PSN == PSN]
                t.stop('get_PSN_PDMs_df')

                # Get all the unique transmit times (assume each unique transmit time is a unique vehicle)
                unique_devices = df_psn.Transmit_To.unique()
                if DEBUG > 1:
                    logger.debug('Found %d unique devices for PSN %s out of %d PDMs' % (len(unique_devices),PSN,len(df_psn)))
                # Loop through each unique transmit time
                for device in unique_devices:
                    df_psn_device = df_psn[df_psn.Transmit_To == device]
                    # Get all the unique transmit times (assume each unique transmit time is a unique vehicle)
                    unique_transmit_times = df_psn_device.Transmit_Time.unique()
                    if DEBUG > 1:
                        logger.debug('Found %d unique transmit times for PSN %s out of %d PDMs at device %s' % (len(unique_transmit_times),PSN,len(df_psn),device))

                    # Loop through each unique transmit time
                    for tt in unique_transmit_times:

                        # Store only the PDMs associated with that unique PSN and transmit time
                        df_veh = df_psn_device[df_psn_device.Transmit_Time == tt]

                        if DEBUG > 1:
                            logger.debug('Found PSN: %s on route group: %s and route: %s at transmit time: %s' % (PSN, route_group, route, tt))
                        travel_time = 0
                        current_time = 0
                        start_time = 0
                        end_time = 0
                        
                        df_veh = df_veh.sort('Time_Taken')
                        # Initialize the hypothical vehicle at the last known position of the PSN
                        last_row = len(df_veh.index) - 1
                        vehicle['PSN'] = PSN
                        vehicle['tp'] = df_veh.iloc[last_row,0]
                        vehicle['x'] = df_veh.iloc[last_row,3]
                        vehicle['y'] = df_veh.iloc[last_row,4]
                        vehicle['v'] = df_veh.iloc[last_row,2] 
                        vehicle['link'] = df_veh.iloc[last_row,5]
                        vehicle['a'] = df_veh.iloc[last_row,6]

                        # Get the time the vehicle ends travel on the route, returns None if not enough messages to find end time or if the vehicle end time exceeds simulation time
                        t.start('get_end_time')
                        end_time = get_end_time(vehicle, df_route_pdms, route_group, route)
                        t.stop('get_end_time')
     
                        if end_time is not None:    
                            if DEBUG > 1:
                                logger.debug("Found end time for PSN: %s traveling %s ft/sec on link: %s at: %s seconds" % (vehicle['PSN'], vehicle['v'], vehicle['link'], end_time))   
                            # Now calculate time traveled to get to the end of the route

                            # ME Travel Time Step #12: Initialize the hypothetical vehicle to the first location of the PSN
                            vehicle['tp'] = df_veh.iloc[0,0]
                            vehicle['x'] = df_veh.iloc[0,3]
                            vehicle['y'] = df_veh.iloc[0,4]
                            vehicle['v'] = df_veh.iloc[0,2] 
                            vehicle['link'] = df_veh.iloc[0,5]
                            vehicle['a'] = df_veh.iloc[0,6]
                            

                            # Get the time the vehicle starts travel on the route, returns None if not enough messages to find start time or if the vehicle start time goes beyond simulation start time
                            t.start('get_start_time')
                            start_time = get_start_time(vehicle, df_route_pdms, route_group, route)
                            t.stop('get_start_time')
                            if start_time > simulation_seconds:
                                start_time = None

                            if start_time is not None:
                                if DEBUG > 1:
                                    logger.debug("Found start time for PSN: %s traveling %s ft/sec on link: %s at: %s seconds" % (vehicle['PSN'], vehicle['v'], vehicle['link'], start_time))
                                total_tt = total_tt + 1

                                # ME Travel Time Step #20: Calculate travel time experienced by the hypothetical vehicle corresponding to the PSN and unique transmit time
                                travel_time = end_time - start_time

                                # Store the travel time for the route according to the vehicles start time
                                time_period_start = get_time_period(start_time)
                                t_times_dict[route_group][route][time_period_start]['tt_list'].append(travel_time)
                                current_datetime = sim_start_time + datetime.timedelta(seconds = int(start_time))
                                start_time = current_datetime.time()
                                current_datetime = sim_start_time + datetime.timedelta(seconds = int(end_time))
                                end_time = current_datetime.time()

                                output_list.append([str(PSN),str(route_group),str(route),str(start_time),str(end_time),str(travel_time)])

                                if DEBUG > 1:
                                    logger.debug('Travel time for vehicle: %s is %s seconds' % (vehicle['PSN'], travel_time))

                if len(output_list) > 500:
                    with open(traveltime_output_name, 'a') as tt_out_f:
                        print 'Writing to file'
                        for line in output_list:
                            tt_out_f.write(','.join(line) + '\n')   
                        output_list = []

    with open(traveltime_output_name, 'a') as tt_out_f:
        if len(output_list) > 0:
            print 'Writing to file'
            for line in output_list:
                tt_out_f.write(','.join(line) + '\n')                     
                            
    print "Number of travel times found: %s" % total_tt
    print "Number of invalid travel times: %s" % invalid_tt_count
    print "Number of travel times not found due to not finding PDMs: %s" % no_tp_due_to_PDMs


    # New travel time method using the average speed of the vehicle and the route length

    use_avg_speed = False
    # Loop through each route

    if use_avg_speed:
        for route_group in routes:
            for route in routes[route_group]:

                # Identify all PDMs on that route
                df_pdms['onRoute'] = df_pdms.apply(lambda row: onRoute(row['Link'], route_group, route), axis = 1)
                df_route_pdms = df_pdms[df_pdms['onRoute'] == True]
                if DEBUG > 0:
                    logger.debug('Found %s PDMs on route_group %s and route %s' % (len(df_route_pdms), route_group, route))

                # Find all the time periods for each PDM
                df_route_pdms['time_period_start'] = df_route_pdms.apply(lambda row: get_time_period(row['Time_Taken']), axis = 1)
                unique_tps = df_route_pdms.time_period_start.unique()

                # Loop through each time period, find the average speed of all vehicles on that route at that time and store
                for time_period in unique_tps:
                    df_tp = df_route_pdms[df_route_pdms.time_period_start == time_period]
                    t_times_dict[route_group][route][time_period]['avg_speed'] = df_tp['Speed'].mean()


    print_ttimes(t_times_dict, routes, simulation_seconds, traveltime_output_name)


def get_weight(row, t):

    if row['Speed'] == 0.0:
        if row['dist'] == 0.0:
            denominator = math.sqrt((t - row['Time_Taken']) ** 2 + (.0001 / 0.0001) ** 2)
        else:
            denominator = math.sqrt((t - row['Time_Taken']) ** 2 + (row['dist'] / 0.0001) ** 2)
    else:
        if row['dist'] == 0.0:
            denominator = math.sqrt((t - row['Time_Taken']) ** 2 + (.0001 / row['Speed']) ** 2)
        else:
            denominator = math.sqrt((t - row['Time_Taken']) ** 2 + (row['dist'] / row['Speed']) ** 2)

    if denominator == 0:
        if DEBUG > 1:
            logger.debug('PDM found with speed=0 and time-time=0')
        return 1
    return 1 / denominator



def get_pdms(vehicle, df_onRoute, time_max, distance_max, route_group, route):
    global df_pdms
    messages = []

    df_time = df_pdms[(abs(vehicle["tp"] - df_pdms["Time_Taken"]) <= time_max)]


    # Calculate the distance of all PDMs from the hypothetical vehicle and determine if the msg is on route
    if len(df_time.index) > 0:
        df_time['dist'] = df_time.apply(lambda row: Chk_Range(vehicle['x'],vehicle['y'],row['X'],row['Y']), axis = 1)

        # Retreive all the valid PDMs
        df_time['onRoute'] = df_time.apply(lambda row: onRouteorNext(row['Link'], route_group, route), axis = 1)
        df_msgs = df_time[(df_time['dist'] <= distance_max) & (df_time['onRoute'] == True)]
    
        if len(df_msgs.index) >= MIN_PDMS:
            if DEBUG > 1:
                logger.debug('Found %s messages after searching over %d seconds and %d feet' % (len(df_msgs.index),time_max,distance_max))
            if DEBUG > 2:
                logger.debug(df_msgs)
            return df_msgs
    
    if time_max > 2400:
        if DEBUG > 1:
            logger.debug('Hit time_max for PSN: %s' % vehicle['PSN'])
        return None

    
    return get_pdms(vehicle = vehicle, df_onRoute = df_onRoute, time_max = time_max + TIME_WINDOW, distance_max = distance_max + DIST_WINDOW, route_group = route_group, route = route)

def travelDistance(origin_x, origin_y, destination_x, destination_y, distance):
    distance_between_points = ((destination_x - origin_x)**2 + (destination_y - origin_y)**2)**.5
    new_x = origin_x + ((distance/distance_between_points) * (destination_x - origin_x))
    new_y = origin_y + ((distance/distance_between_points) * (destination_y - origin_y))
    return new_x, new_y


def getLinkPosition(link):
    global link_positions
    for l in link_positions.keys():
        if link == l:
            x = link_positions[link]["x"]
            y = link_positions[link]["y"]
            return x,y


def psnOnRoute(df_psn, route_group, route):
    global full_routes
    # Check that the PSN starts and ends on a link within the route
    if df_psn.iloc[0,5] in full_routes[route_group][route] and df_psn.iloc[len(df_psn.index)-1,5] in full_routes[route_group][route]:
        return True
    else:
        return False

def onRoute(link, route_group, route):
    global full_routes
    for l in full_routes[route_group][route]:
        if l == link:
            return True
    return False

def onRouteorNext(link, route_group, route):
    global full_routes
    global downstream_superlinks
    for l in full_routes[route_group][route]:
        if l == link:
            return True
    for l in full_routes[downstream_superlinks[route_group][route]['route_group']][downstream_superlinks[route_group][route]['route']]:
        if l == link:
            return True
    return False

def onNextSuperlink(link, route_group, route):
    global downstream_superlinks
    for l in full_routes[downstream_superlinks[route_group][route]['route_group']][downstream_superlinks[route_group][route]['route']]:
        if l == link:
            return True

def get_time_period(tp):
    return ((tp - (tp % SECONDS_INTERVAL))/ SECONDS_INTERVAL) + 1 #300 seconds for 5-minute intervals, 60 seconds for 1-minute intervals


def get_prev_link(link, route_group, route):
    global full_routes
    if full_routes[route_group][route].index(link) != 0:
        return int(full_routes[route_group][route][full_routes[route_group][route].index(link)-1])
    else:
        return None


def get_next_link(link, route_group, route):
    global full_routes
    return int(full_routes[route_group][route][full_routes[route_group][route].index(link)+1])


def print_ttimes(t_times_dict, routes, simulation_seconds, traveltime_output_name):
    global dir_path
    output_name = traveltime_output_name.split('.csv')
    filename = output_name[0] + '_aggregated.csv'
    print "Writing aggregated output to file:%s" % filename
    start_time = datetime.datetime(100,1,1,int(START_HOUR),0,0)

    
    with open(filename, 'wb') as out_file:
        out_file.write('Route_Group,Route,Simulation_time,NN_Average_Travel_Time,Num_PSNs,Route_length,Average_Speed,Average_Speed_Travel_Time\n')
        for route_group in t_times_dict:
            for route in t_times_dict[route_group]:
                for time_period_start in t_times_dict[route_group][route]:

                    # ME Travel Time Step #21: For each time period, calculate the average travel time experienced by a vehicle
                    num_veh = len(t_times_dict[route_group][route][time_period_start]['tt_list'])
                    if num_veh > 0:
                        total = 0
                        for time in t_times_dict[route_group][route][time_period_start]['tt_list']:
                            total = total + time
                        avg_tt = total / num_veh
                    else:
                        avg_tt = 'NA'

                    current_datetime = start_time + datetime.timedelta(seconds = int(time_period_start * SECONDS_INTERVAL))
                    current_time = current_datetime.time()

                    # New Method
                    use_avg_speed = False
                    if use_avg_speed:
                        route_length = float(get_route_length(route_group, route))
                        avg_spd = t_times_dict[route_group][route][time_period_start]['avg_speed']
                        if avg_spd != 'NA':
                            calc_tt = "{0:.2f}".format(route_length / avg_spd)
                        else:
                            calc_tt = 'NA'
                            
                        out_file.write('%s,%s,%s,%s,%s,%s,%s,%s\n' %(route_group, route, current_time, avg_tt, num_veh, route_length, avg_spd, calc_tt))

                    else:
                        out_file.write('%s,%s,%s,%s,%s\n' %(route_group, route, current_time, avg_tt, num_veh))
            
def move_vehicle(dist_traveled, vehicle, route_group, route, isFindStartTime):
    
    while (True):
        
        dist_to_link_end = Chk_Range(vehicle['x'], vehicle['y'], vehicle['next_x'], vehicle['next_y'])
        if DEBUG > 1:
            logger.debug('Vehicle: %s is traveling %s feet with %s feet left on link %s at time: %s' % (vehicle['PSN'], dist_traveled, dist_to_link_end, vehicle['link'], vehicle['tp']))


        # If the vehicle is traveling farther than the length of the current link
        if dist_traveled >= dist_to_link_end:

            # Find the time it takes for the vehicle to travel the remaining length of the current link
            current_link_travel_time = dist_to_link_end / vehicle['v']

            # Subtract the remaining link length from the total distance traveled
            dist_traveled = dist_traveled - dist_to_link_end

            # Set vehicle location to the next x,y of the current link since the vehicle reached the end
            vehicle['x'] = vehicle['next_x']
            vehicle['y'] = vehicle['next_y']

            # Record the new time of the vehicle (subtract if finding the start time)
            if isFindStartTime:
                vehicle['tp'] = vehicle['tp'] - current_link_travel_time
            else:
                vehicle['tp'] = vehicle['tp'] + current_link_travel_time
            
            # ME Travel Time Step #11 and Step #19: Check if the vehicle has reached the route origin or destination
            if (isFindStartTime and (vehicle['link'] == routes[route_group][route]['route_origin'])) or (not isFindStartTime and (vehicle['link'] == routes[route_group][route]['route_destination'])):
                # Return the current vehicle time and the vehicle
                return vehicle['tp'], vehicle

            # Else, put the hypothetical vehicle on the next link 
            else:

                # If finding the start time, the next link is the previous link on the route
                if isFindStartTime:
                    new_current_link = get_prev_link(vehicle['link'], route_group, route)
                    prev_link = get_prev_link(new_current_link, route_group, route)
                    # Set new origin x,y coordinates
                    if prev_link is not None:
                        vehicle['next_x'], vehicle['next_y'] = getLinkPosition(prev_link)
                    else: # vehicle is on the origin link
                        vehicle['next_x'] = routes[route_group][route]['x']
                        vehicle['next_y'] = routes[route_group][route]['y']
                # Else put the vehicle on the next link in the route
                else:
                    new_current_link = get_next_link(vehicle['link'], route_group, route)
                    if DEBUG > 1:
                        logger.debug(new_current_link)
                    # Set new destination coordinates
                    vehicle['next_x'], vehicle['next_y'] = getLinkPosition(new_current_link)
                vehicle['link'] = new_current_link

        # Else, the vehicle remains on the current link
        else:
            #if float(vehicle['v']) != 0.0:
            current_link_travel_time = dist_traveled / vehicle['v']
            #else:
             #   current_link_travel_time = 4
            if isFindStartTime:
                vehicle['tp'] = vehicle['tp'] - current_link_travel_time
            else:
                vehicle['tp'] = vehicle['tp'] + current_link_travel_time

            # Move the vehicle along the distance traveled between the current x,y and the end of the link, set the next location of the vehicle
            vehicle['x'], vehicle['y'] = travelDistance(vehicle['x'], vehicle['y'], vehicle['next_x'], vehicle['next_y'], dist_traveled)

            if DEBUG > 1:
                logger.debug('Vehicle updated to:')
                logger.debug(vehicle)

            return None, vehicle

def get_start_time(vehicle, df_pdms, route_group, route):
    global routes
    global invalid_tt_count
    global no_tp_due_to_PDMs

    #Determine the origin x,y coordinates of the current link
    if vehicle['link'] != routes[route_group][route]['route_origin']:
        prev_link = get_prev_link(vehicle['link'], route_group, route)
        vehicle['next_x'], vehicle['next_y'] = getLinkPosition(prev_link)
    else:
        vehicle['next_x'] = routes[route_group][route]['x']
        vehicle['next_y'] = routes[route_group][route]['y']
    
    # Find the distance traveled by the vehicle at the current speed
    dist_traveled = (vehicle['v'] * 4)

    # Loop through each 4 seconds of hypothetical vehicle travel
    while (True):
        if vehicle['tp'] < MIN_TIME:
            invalid_tt_count = invalid_tt_count + 1
            if DEBUG > 1:
                logger.debug('Vehicle tp below the 15 minutes threshold')
            return None

        # ME Travel Time Step #18: Move the hypothetical vehicle the distance traveled to new location on the route 
        start_time, vehicle = move_vehicle(dist_traveled, vehicle, route_group, route, isFindStartTime = True)

        # If the vehicle reached the end of the route, return the start time
        if start_time is not None:
            return start_time 

        # ME Travel Time Step #13: Search for all messages generated on the route within a pre-defined time-space region from the current location
        t.start('get_pdms')
        df_msgs = get_pdms(vehicle = vehicle, df_onRoute = df_pdms, time_max = TIME_WINDOW, distance_max = DIST_WINDOW, route_group = route_group, route = route)
        t.stop('get_pdms')

        # If no messages found, travel time using this PSN is not possible because there are not enough PDMs
        if df_msgs is None:
            if DEBUG > 1:
                logger.debug('Not enough PDMs to move vehicle: %s past link: %s' % (vehicle['PSN'], vehicle['link']))
            no_tp_due_to_PDMs = no_tp_due_to_PDMs + 1
            return None

        # ME Travel Time Step #15: Find the weight of each PDM
        t.start('find_weight')
        df_msgs['w'] = df_msgs.apply(lambda row: get_weight(row, vehicle['tp']), axis = 1)

        # ME Travel Time Step #16: Rank order all messages identified in Step #13 in descending order based on weights
        df_msgs = df_msgs.sort(columns = 'w', ascending = False)
        t.stop('find_weight')

        # Travel Time Step #17: Find the new speed of the hypothetical vehicle
        t.start('find_new_speed')
        if len(df_msgs.index) >= MAX_PDMS:
            df_msgs = df_msgs[:MAX_PDMS]
        df_msgs['vw'] = df_msgs['Speed'] * df_msgs['w']
        vehicle['v'] = df_msgs['vw'].sum() / df_msgs['w'].sum()
        t.stop('find_new_speed')
        dist_traveled = vehicle['v'] * 4
        if DEBUG > 1:
            logger.debug('Vehicle speed changed to: %s at tp: %s on link: %s' % (vehicle['v'], vehicle['tp'], vehicle['link']))
        
    # End While
        


def get_end_time(vehicle, df_pdms, route_group, route):
    
    global invalid_tt_count
    global no_tp_due_to_PDMs

    vehicle['next_x'], vehicle['next_y'] = getLinkPosition(vehicle['link'])

    dist_to_link_end = Chk_Range(vehicle['x'], vehicle['y'], vehicle['next_x'], vehicle['next_y'])

    # ME Travel Time Step #4: Find the distance traveled by the hypothetical vehicle given the vehicle's speed and acceleration
    dist_traveled = (vehicle['v'] * 4) #+ (0.5 * vehicle['a'] * 4 * 4) # ut + 1/2 a t^2
    
    # Loop through each 4 seconds of hypothetical vehicle travel
    while (True):
        # Set the new location of the vehicle given the distance traveled. Return end time if vehicle reaches the end of the route
        end_time, vehicle = move_vehicle(dist_traveled, vehicle, route_group, route, isFindStartTime = False)
        if end_time is not None:
            return end_time
                
        # ME Travel Time Step #5: Find all messages generated on the route within a pre-defined time-space region from the vehicle's position
        t.start('get_pdms')
        df_msgs = get_pdms(vehicle = vehicle, df_onRoute = df_pdms, time_max = TIME_WINDOW, distance_max = DIST_WINDOW, route_group = route_group, route = route)
        t.stop('get_pdms')

        if df_msgs is None:
            # If no messages found, travel time using this PSN is not possible because there are not enough PDMs
            no_tp_due_to_PDMs = no_tp_due_to_PDMs + 1
            return None

        # ME Travel Time Step #7 - Find the weight of each PDM
        t.start('find_weight')
        df_msgs['w'] = df_msgs.apply(lambda row: get_weight(row, vehicle['tp']), axis = 1)
        
        # ME Travel Time Step #8: Rank order all messages in descending order based on the weights
        df_msgs = df_msgs.sort(columns = 'w')
        t.stop('find_weight')
        
        # Travel Time Step #9 - Find the new speed of the hypothetical vehicle, using no more than the defined max number of messages
        t.start('find_new_speed')
        if len(df_msgs.index) >= MAX_PDMS:
            df_msgs = df_msgs[:MAX_PDMS]
        df_msgs['vw'] = df_msgs['Speed'] * df_msgs['w']
        vehicle['v'] = df_msgs['vw'].sum() / df_msgs['w'].sum()
        t.stop('find_new_speed')

        # ME Travel Time Step #10: Find the new distance traveled based on the new speed.
        dist_traveled = vehicle['v'] * 4
        if DEBUG > 1:
            logger.debug('Vehicle speed changed to: %s at tp: %s on link: %s' % (vehicle['v'], vehicle['tp'], vehicle['link']))
        
    # End While

def get_route_length(route_group, route):
    global full_routes, link_lengths
    route_length_sum = 0
    for link in full_routes[route_group][route]:

        if link in link_lengths.keys():
            route_length_sum = route_length_sum + link_lengths[link]

        else:
            print "Error: Link %s not found" % link
            exit(0)
    return route_length_sum

def on_superlinks(veh_link):
    global full_routes
    for route_group in full_routes.keys():
        for route in full_routes[route_group].keys():
            for link in full_routes[route_group][route]:
                if int(link) == int(veh_link): 
                    return 1

    return 0

def read_downstream(filename):
    global downstream_superlinks
    with open(filename, 'r') as in_f:
        for line in in_f:
            row = line.strip().split(',')

            if row[0] not in downstream_superlinks.keys():
                downstream_superlinks[row[0]] = {} # Key is superlink/bottleneck name
            if row[1] not in downstream_superlinks[row[0]].keys():
                downstream_superlinks[row[0]][row[1]] = {}

            downstream_superlinks[row[0]][row[1]]['route_group'] = row[2]
            downstream_superlinks[row[0]][row[1]]['route'] = row[3] 


t.start('main')

parser = argparse.ArgumentParser(description='ME program for reading in BSMs and producing Travel Time values')
parser.add_argument('pdm_filename') # CSV file of PDMs
parser.add_argument('routes_filename') # CSV file of route origins and destinations
parser.add_argument('fullroutes_filename') # CSV file of full routes
parser.add_argument('link_positions_filename') # CSV file of link end points
parser.add_argument('length_filename') # CSV file of link lengths
parser.add_argument('downstream_filename') # CSV file of link lengths
parser.add_argument('simulation_seconds') # Number of simulation seconds 
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

routes = read_routes(args.routes_filename)

full_routes = read_full_routes(args.fullroutes_filename)

link_lengths = read_link_length(args.length_filename)

link_positions = read_endpoints(args.link_positions_filename)

simulation_seconds = int(args.simulation_seconds)

read_downstream(args.downstream_filename)

if DEBUG > 0:
    logger.info("Processing PDM file:%s" % args.pdm_filename)

print "Processing PDM file:%s" % args.pdm_filename

df_pdms = pd.read_csv(filepath_or_buffer=args.pdm_filename,header=0,skipinitialspace = True,usecols=['Time_Taken','PSN','Speed','X','Y','Link','Acceleration','Transmit_Time','Transmit_To'])

df_pdms = df_pdms[df_pdms['Time_Taken'] >= MIN_TIME]
df_pdms = df_pdms[df_pdms['Time_Taken'] <= simulation_seconds + 1400]

if args.out:
    out_file = dir_path + '/' + args.out
else:
    out_file = dir_path + '/traveltime_me_PDM_output.csv'

if DEBUG > 0:
    logger.info('Running simulation')

    
run_ME_traveltime_PDMs(df_pdms, routes, simulation_seconds, out_file)

t.stop('main')


if t.enabled:
    print t['main']
    with open(dir_path + '/timeit_' + args.out, 'wb') as time_f:
        time_f.write(t.header())
        time_f.write(t.write())
        time_f.write('\n\n')