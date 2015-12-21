#standard
import argparse
import datetime
import os

#local
from readlinks import read_traveltimes_file, read_speed_limit, read_link_length, read_full_routes 
from TCACore import Timer

t = Timer(enabled=True)
START_HOUR = 17

def run_gt_delay(travel_times, full_routes, link_lengths, speed_limits, delay_output_name = '/gluster/gluster1/bsm_data_emulator/GT_code/Delay/gt_delay.csv'):

    freeflow_times = {} #Stores free flow travel times
    delay_output = [] #Stores difference between free flow and average travel times

    #Add header to output file
    with open(delay_output_name, 'wb') as out_f:
        out_f.write('route_group,route,time,average_delay,average_travel_time,free_flow_travel_time\n')


    with open(delay_output_name, 'a') as out_f:
        #Delay Step 4:  Calculate the travel time at posted speeds limits 
        for route_group in full_routes.keys():
            if route_group not in freeflow_times.keys():
                freeflow_times[route_group] = {}
            for route in full_routes[route_group].keys():
                travel_time = 0.0
                speed = 0.0

                for link in full_routes[route_group][route]:
                    #Get posted speed limit for link, if no speed limit given for link, use previous speed value
                    speed = check_speed(link, speed_limits, speed)
                    if speed == 0.0:
                        #If there is no speed limit on the initial link produce error message
                        print "Error: Link %.1f not found" % link
                        exit(0)
                    #Find the length of the link divided by the posted speed limit on the link to get the free flow
                    #travel time on that link and add it to the overall travel time on the route
                    link_length = get_link_length(link, link_lengths)
                    travel_time += link_length/speed
                #Once every link in the route has been processed, record the free flow travel time
                freeflow_times[route_group][route] = travel_time
        #Delay Step 6: For each time, t, calculate average delay experienced by a vehicle on Route R when starting trip between t-1  to t minutes as follows:
        print freeflow_times
        for route_group in travel_times.keys():
            for route in travel_times[route_group].keys():
                for minute in travel_times[route_group][route].keys():
                    travel_time = travel_times[route_group][route][minute]
                    delay_time = freeflow_times[route_group][route]
                    if travel_time != 'NA':
                        average_delay = float(travel_time) - float(delay_time) 
                    else:
                        average_delay = 'NA'
                    #Delay Step 6: Print route, time, average delay, average travel time and free flow travel time                
                    delay_output.append(([str(route_group),str(route),str(minute),str(average_delay),str(travel_time),str(delay_time)]))

        # Write output to file
        write_output(out_f, delay_output)
           


def write_output(out_f, output):

  if len(output) > 0:
      print 'Writing to file'
      for line in output:
           out_f.write(','.join(line) + '\n')

def check_speed(link, speed_limits, speed):
    #Checks the speed limit input for given link and returns speed limit on that link
    #if it is found. Otherwise, returns speed value that was passed to it. 
    for l in speed_limits.keys():
        if link == l:
            speed = speed_limits[l]
            return speed
    return speed

def get_link_length(link, link_lengths):
    #Checks the link length input file for the given link and returns the link's length.
    for l in link_lengths.keys():
        if link == l:
            length = link_lengths[l]
            return length

    print "Error: Link %.1f not found", link
    exit(0)

t.start('main')

parser = argparse.ArgumentParser(description='GT program for reading in csv files and producing Delay values')
parser.add_argument('traveltime_filename') # CSV file of average travel time output
parser.add_argument('full_routes_filename') # CSV file of full routes
parser.add_argument('length_filename') # CSV file of link lengths
parser.add_argument('speed_filename') # CSV file of speed limits
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

#Delay Step 1: Follow steps 1-4 detailed under Ground Truth Algorithm for Travel Time to calculate the average travel time 
travel_time = read_traveltimes_file(args.traveltime_filename)

full_routes = read_full_routes(args.full_routes_filename)

link_lengths = read_link_length(args.length_filename)

speed_limits = read_speed_limit(args.speed_filename)

if args.out:
    out_file = dir_path + '/' + args.out
else:
    out_file = dir_path + '/gt_delay.csv'

#Delay Step 2: For each Route R, do steps 4 to 6.
run_gt_delay(travel_time, full_routes, link_lengths, speed_limits, out_file)

t.stop('main')
print t['main']