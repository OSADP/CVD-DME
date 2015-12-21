#standard
import argparse
import os

#local
from GTFileReader import Trajectories
from readlinks import read_routes, read_link_length
#from TCACore import Timer

#t = Timer(enabled=True)
SECONDS_INTERVAL = 300
BUFFER_DISTANCE = 50

def run_gt_travel_time_new(trj, routes, link_lengths, traveltime_output_name):
    traveltime_output = []

    route_vehicles = {}

    #initialize list to hold origin and destination times for each vehicle
    for route_group in routes.keys():
        route_vehicles[route_group] = {}
        for route in routes[route_group].keys():
            route_vehicles[route_group][route] = {}

    with open(traveltime_output_name, 'wb') as out_f:
        out_f.write('route_group,route, simulation_time, average_travel_time\n')

    with open(traveltime_output_name, 'a') as out_f:

        # Travel Time Step 2: Find vehicles that start trip on L_O^R and end trip on L_D^R from t-1 to t. 
        for tp, veh_list in trj.read():
            # Find time in minutes   
            time = ((tp - (tp % SECONDS_INTERVAL))/SECONDS_INTERVAL) + 1

            for route_group in routes.keys():
                # Travel Time Step 2: Identify IDs of all vehicles that are in lane l, on link L_O^R from time t-1 to t
                origin_veh = get_origin_vehicles(routes[route_group][routes[route_group].keys()[0]]['route_origin'], veh_list)
                for route in routes[route_group].keys():

                    if time not in route_vehicles[route_group][route]:
                        route_vehicles[route_group][route][time] = {}

                    # Travel Time Step 2: For each vehicle, i, get t_i^(R,O).
                    for vehicle in origin_veh:
                        if vehicle['vehid'] not in route_vehicles[route_group][route][time].keys():
                            route_vehicles[route_group][route][time][vehicle['vehid']] = {}
                            route_vehicles[route_group][route][time][vehicle['vehid']]['start_time'] = tp

                    # Travel Time Step 2: Identify IDs of all vehicles that are in lane l, on link L_D^R from time t - 1 to t       
                    destination_veh = get_destination_vehicles(routes[route_group][route]['route_destination'], veh_list, link_lengths)

                    # Travel Time Step 2: For each vehicle, i, get t_i^(R,D).
                    for veh in destination_veh:
                        for minute in route_vehicles[route_group][route].keys():
                            # Only find destination time if the vehicle was found at the route origin
                            if veh['vehid'] in route_vehicles[route_group][route][minute].keys():
                                route_vehicles[route_group][route][minute][veh['vehid']]['stop_time'] = tp
                                break

        # Travel Time Step 3: For time t in T for each i in I(L_O^R,l,t), check the last time, t_i^(R,D), the vehicle is found on link L_D^R. For each such vehicle, calculate vehicle-specific travel time on Route R              
        for route_group in route_vehicles.keys():
            for route in route_vehicles[route_group].keys():
                for time in route_vehicles[route_group][route].keys():
                    m = 0
                    travel_times = 0
                    for vehid in route_vehicles[route_group][route][time].keys():
                        # Only find the travel time if the vehicle was found at the origin and destination links of the route
                        if 'start_time' in route_vehicles[route_group][route][time][vehid].keys() and 'stop_time' in route_vehicles[route_group][route][time][vehid].keys():
                            travel_times += (route_vehicles[route_group][route][time][vehid]['stop_time'] - route_vehicles[route_group][route][time][vehid]['start_time'])
                            m = m + 1

                    # Time Travel Step 4: Calculate average travel time experienced by a vehicle on Route R when starting trip between t-1 to t minutes
                    if m > 0:
                        average_travel_time = travel_times/m
                    else:
                        average_travel_time = 'NA'
                    # Time Travel Step 4: Print R, t, Att_t^R
                    traveltime_output.append(([str(route_group), str(route), str(time), str(average_travel_time), str(m)]))

        # Write output to file
        write_output(out_f, traveltime_output)
           


def write_output(out_f, output):

  if len(output) > 0:
      print 'Writing to file'
      for line in output:
           out_f.write(','.join(line) + '\n')

def distance_between(origin_x, origin_y, destination_x, destination_y):
    distance = ((origin_x - destination_x)**2 + (origin_y - destination_y)**2)**.5
    return distance

def get_origin_vehicles(roadway, veh_list):

    # Create list of vehicles on link provided (roadway)
    roadway_veh = []
    #Get vehicles within 50 feet of the start of the origin link
    for veh in veh_list:
        if int(veh['Link']) == int(roadway):
            if veh['x'] <= BUFFER_DISTANCE:
                    roadway_veh.append(veh)

    return roadway_veh

def get_destination_vehicles(roadway, veh_list, link_lengths):

    # Create list of vehicles on link provided (roadway)
    roadway_veh = []
    #Get vehicles within 50 feet of the end of the destination link
    for veh in veh_list:
        if (int(veh['Link']) == int(roadway) and veh['x'] >= get_link_length(roadway, link_lengths) - BUFFER_DISTANCE):
            roadway_veh.append(veh)

    return roadway_veh

def get_link_length(link, link_lengths):

    for l in link_lengths.keys():
        if link == l:
            length = link_lengths[l]
            return length

    print "Error: Link %.1f not found" % link
    exit(0)


#t.start('main')

parser = argparse.ArgumentParser(description='GT program for reading in fzp files and producing Travel Time values')
parser.add_argument('trj_filename') # FZP file of vehicle trajectories
parser.add_argument('routes_filename') # CSV file of routes
parser.add_argument('length_filename') # CSV file of link lengths 
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

trj = Trajectories(filename=args.trj_filename)

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

#Travel Time Step 1: For each Route, R in the network, find L_O^R and L_D^R.  
routes = read_routes(args.routes_filename)

link_lengths = read_link_length(args.length_filename)

if args.out:
    out_file = dir_path + '/' + args.out
else:
    out_file = dir_path + '/traveltime_gt.csv'

run_gt_travel_time_new(trj, routes, link_lengths, out_file)
#t.stop('main')
#print t['main']