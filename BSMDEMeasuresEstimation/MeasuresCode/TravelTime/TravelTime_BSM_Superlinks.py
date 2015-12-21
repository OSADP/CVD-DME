activate_this = '/gluster/gluster1/bsm_data_emulator/bsmde_python/bin/activate_this.py'
try:
   execfile(activate_this, dict(__file__=activate_this))
except:
    pass

#standard
import argparse
import pandas as pd
import os

os.environ['TMPDIR'] = '/var/tmp'

from readlinks import read_routes, read_link_length, read_full_routes, read_endpoints
from TCACore import Timer

t = Timer(enabled=True)

link_lengths = {}
link_positions = {}
full_routes = {}
downstream_superlinks = {}
MIN_BSM = 4
TIME_WINDOW = 5
DISTANCE_WINDOW = 20

def run_travel_time(df_bsms, routes, simulation_seconds,traveltime_output_name):
	traveltime_output = []

	with open(traveltime_output_name, 'wb') as out_f:
		out_f.write('route_group,route, simulation_time, average_travel_time\n')

	for route_group in routes:
		for route in routes[route_group]:
			df_bsms["onRoute"] = df_bsms.apply(lambda row: onRoute(row["link"], route_group, route), axis=1)
			df_onRoute = df_bsms[(df_bsms["onRoute"] == True)]
			for start_time in range(0,int(simulation_seconds),300):
				vehicle = initialize_vehicle(routes, start_time, route_group, route)
				while vehicle["end_time"] is None:
					messages = get_bsms(vehicle, df_onRoute, TIME_WINDOW, DISTANCE_WINDOW, route_group, route)
					message_weights = []
					if messages is None:
						print "Output not completed ", vehicle 
						break
					for message in messages:
						time_diff = (vehicle["tp"] - message[2])**2
						dis = distance_between(vehicle["x"], vehicle["y"], message[4], message[5])
						if dis == 0.0:
							dis = 0.0001
						if message[3] != 0.0:
							dis_diff = (dis/message[3])**2
						else:
							speed = 0.0001
							dis_diff = (dis/speed)**2
						message_weights.append([message, 1/(time_diff + dis_diff)**.5])
					sorted(message_weights, reverse=True, key=getKey)
					new_speed_numerator = 0
					new_speed_denominator = 0
					for i in range (0,min(len(message_weights),8)):
						new_speed_numerator += message_weights[i][1] * message_weights[i][0][3]
						new_speed_denominator += message_weights[i][1]
					new_speed = new_speed_numerator/new_speed_denominator
					distance_to_travel = new_speed * 4 
					vehicle["tp"] += 4
					move_vehicle(vehicle, new_speed, distance_to_travel, vehicle["link"], route_group, route)
				if vehicle["end_time"] is not None:
					average_travel_time = (vehicle["end_time"] - start_time)
					traveltime_output.append(([str(route_group), str(route), str((start_time/60)+1), str(average_travel_time)]))
				else:
					traveltime_output.append(([str(route_group), str(route), str((start_time/60)+1), "NA"]))
				if start_time % 120 == 0:
					print start_time
	with open(traveltime_output_name, 'a') as out_f:
	    # Write output to file
	    write_output(out_f, traveltime_output)
           


def write_output(out_f, output):

  if len(output) > 0:
      print 'Writing to file'
      for line in output:
           out_f.write(','.join(line) + '\n')

def getKey(item):
	return item[1]

def distance_between(origin_x, origin_y, destination_x, destination_y):
	distance = ((origin_x - destination_x)**2 + (origin_y - destination_y)**2)**.5
	return distance

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
		return get_bsms(vehicle, df_bsms, time_max + TIME_WINDOW, distance_max + DISTANCE_WINDOW, route_group, route)


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

def travelDistance(origin_x, origin_y, destination_x, destination_y, distance):
	distance_between_points = ((destination_x - origin_x)**2 + (destination_y - origin_y)**2)**.5
	new_x = origin_x + ((distance/distance_between_points) * (destination_x - origin_x))
	new_y = origin_y + ((distance/distance_between_points) * (destination_y - origin_y))
	return new_x, new_y

def initialize_vehicle(routes, start_time, route_group, route):
	vehicles = {}   
	if route_group not in vehicles.keys():
		vehicles[route_group] = {}		
	if route not in vehicles[route_group].keys():
		vehicles[route_group][route] = {}
	vehicles[route_group][route][start_time] = {}
	vehicles[route_group][route][start_time]["dist_traveled"] = 0
	vehicles[route_group][route][start_time]["x"] = routes[route_group][route]['x']
	vehicles[route_group][route][start_time]["y"] = routes[route_group][route]['y']
	vehicles[route_group][route][start_time]["link"] = routes[route_group][route]['route_origin']
	vehicles[route_group][route][start_time]["tp"] = start_time
	vehicles[route_group][route][start_time]["end_time"] = None 
	return vehicles[route_group][route][start_time]

def finishTrip(vehicle, distance, new_speed):
	vehicle["end_time"] = vehicle["tp"] - (distance/new_speed)

def get_link_length(link):
	global link_lengths
	for l in link_lengths.keys():
		if link == l:
			length = link_lengths[l]
	return length

def getLinkPosition(link):
	global link_positions
	for l in link_positions.keys():
		if link == l:
			x = link_positions[link]["x"]
			y = link_positions[link]["y"]
			return x,y

def getNextLink(link, route_group, route):
	global full_routes
	flag = False
	for l in full_routes[route_group][route]:
		if flag:
			return l
		if l == link:
			flag = True


def onRoute(link, route_group, route):
    global full_routes
    global downstream_superlinks
    for l in full_routes[route_group][route]:
        if l == link:
            return True
    for l in full_routes[downstream_superlinks[route_group][route]['route_group']][downstream_superlinks[route_group][route]['route']]:
        if l == link:
            return True
    return False

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
parser.add_argument('bsm_filename') # CSV file of Basic Safety Messages
parser.add_argument('routes_filename') # CSV file of route origins and destinations
parser.add_argument('fullroutes_filename') # CSV file of full routes
parser.add_argument('link_positions_filename') # CSV file of link end points
parser.add_argument('length_filename') # CSV file of link lengths
parser.add_argument('downstream_filename') # CSV file of link lengths
parser.add_argument('simulation_seconds') # CSV file of link lengths
parser.add_argument('--out', help = 'Output csv file (include .csv)')  
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

df_bsms = pd.read_csv(filepath_or_buffer = args.bsm_filename, header = 0, skipinitialspace = True, usecols = ['localtime', 'spd', 'x', 'y', 'link'])

df_bsms = df_bsms.rename(columns={'spd': 'speed'})

df_bsms['speed'] = df_bsms['speed'].apply(lambda x: x * 1.46667)

routes = read_routes(args.routes_filename)

full_routes = read_full_routes(args.fullroutes_filename)

link_lengths = read_link_length(args.length_filename)

link_positions = read_endpoints(args.link_positions_filename)

read_downstream(args.downstream_filename)

if args.out:
    out_file = dir_path + '/' + args.out

else:
    out_file = dir_path + '/traveltime_bsm.csv'

run_travel_time(df_bsms, routes, args.simulation_seconds, out_file)

t.stop('main')
print t['main']