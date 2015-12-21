#standard
import argparse
import datetime
import os

#local
from GTFileReader import Trajectories
from readlinks import read_full_routes, read_traveltimes_file, read_link_length
from TCACore import Timer

t = Timer(enabled=True)
START_HOUR = 17

def run_gt_speed(superlinks, link_lengths, travel_times, speed_output_name):

    speed_output = []
    start_time = datetime.datetime(100,1,1,int(START_HOUR),0,0)

    with open(speed_output_name, 'wb') as out_f:
        out_f.write('time,roadway,lane,space_mean_speed,roadway_length\n')


    with open(speed_output_name, 'a') as out_f:

        tp_count  = 0

        speed_data = {}
        link_length_data = {}
        # Set route_group length, initialize speed sum and VID count for each lane/route_group at start of algorithm
        for route_group in superlinks:
            link_length_data[route_group] = {}
            for route_num in superlinks[route_group].keys():
                link_length = 0
                for i in range(len(superlinks[route_group][route_num])):
                    link_length += float(link_lengths[superlinks[route_group][route_num][i]])
                link_length_data[route_group][route_num] = link_length


        for route_group in superlinks:
            for route_num in travel_times[route_group].keys():
                # Speed GT Step #5: Calculate the space mean speed = Ls / average travel time for each timestep (Step #1)            
                for minute in travel_times[route_group][route_num].keys():
                    if travel_times[route_group][route_num][minute] != 'NA':
                        space_mean_speed = (link_length_data[route_group][route_num]) / float(travel_times[route_group][route_num][minute])
                    else:
                        space_mean_speed = 'NA' # No average travel time data for this minute on this route_group and/or lane
                    speed_output.append([str(minute), str(route_group), str(route_num), str(space_mean_speed), str(link_length_data[route_group][route_num])])

        write_output(out_f, speed_output)

def write_output(out_f, output):
  if len(output) > 0:
      print 'Writing to file'
      for line in output:
           out_f.write(','.join(line) + '\n')


t.start('main')

parser = argparse.ArgumentParser(description='GT program for reading in traveltime files and producing Speed values')
parser.add_argument('traveltime_filename', help = 'File of travel times for each superlink (output of Travel time GT algorithm)')
parser.add_argument('full_superlinks_filename', help = 'CSV file of super links') 
parser.add_argument('link_length_filename', help = 'CSV file of link lengths')
parser.add_argument('--out', help = 'Output csv file (include .csv)')
args = parser.parse_args()

dir_path = os.path.dirname( os.path.realpath( __file__ ) )

full_superlinks = read_full_routes(args.full_superlinks_filename)

link_lengths = read_link_length(args.link_length_filename)

travel_times = read_traveltimes_file(args.traveltime_filename)

if args.out:
    out_file = dir_path + '/' + args.out
else:
    out_file = dir_path + '/gt_speed.csv'

run_gt_speed(full_superlinks, link_lengths, travel_times, out_file)

t.stop('main')
print t['main']