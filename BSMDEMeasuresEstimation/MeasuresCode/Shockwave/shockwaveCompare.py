import argparse
def read_shockwaves(shockwaves_file):
	shockwaves = []
	header_flag = True
	with open(shockwaves_file, "r") as in_f:
		for line in in_f:
			if header_flag:
				header_flag = False
				continue
			data = line.split(',')
			shockwave = {
			  'time': float(data[0].strip()),
              'link': data[1].strip(),
              'lane': int(data[2].strip()),
              'start_location': float(data[3].strip()),
              'shockwave_length': float(data[4].strip()),
              'end_time': float(data[5].strip()),
              'end_x': float(data[6].strip()),
              'shockwave_type': data[7].strip(),
              'shockwave_propogation_speed': data[8].strip(),
              'shockwave_count': data[9].strip(),
              'significant_shockwave': data[10].strip(),
            }
			shockwave['duration'] = shockwave['end_time'] - shockwave['time']
			shockwaves.append(shockwave)
	return shockwaves

parser = argparse.ArgumentParser(description='ME program for reading in BSMs and producing Travel Time values')
parser.add_argument('gt_filename') # CSV file of PDMs
parser.add_argument('me_filename') # CSV file of route origins and destinations
parser.add_argument('me_filename2') # CSV file of route origins and destinations
parser.add_argument('gt_output')
parser.add_argument('me_output')
args = parser.parse_args()

gt_shockwave_output_file = args.gt_output
me_shockwave_output_file = args.me_output


gt_shockwaves = read_shockwaves(args.gt_filename)
me_shockwaves = read_shockwaves(args.me_filename)
me_shockwaves2 = read_shockwaves(args.me_filename2)

gt_shockwave_output = []
me_shockwave_output = []
count = 0
no_count = 0
with open("nomatch.csv", "wb") as out_f:
	for gt_shockwave in gt_shockwaves:
		shockwave_found = False
		start = False
		end = False
		for me_shockwave in me_shockwaves:
			if abs(gt_shockwave["time"] - me_shockwave["time"]) <= 30 and abs(gt_shockwave["start_location"] - me_shockwave["start_location"]) <= 50:
				if abs(gt_shockwave["end_time"] - me_shockwave["end_time"]) <= 30 and abs(gt_shockwave["end_x"] - me_shockwave["end_x"]) <= 50:
					gt_shockwave_output.append(gt_shockwave)
					me_shockwave_output.append(me_shockwave)
					shockwave_found = True
		if shockwave_found:
			count = count + 1
		else: 
			for me_shockwave in me_shockwaves2:
				if abs(gt_shockwave["time"] - me_shockwave["time"]) <= 30 and abs(gt_shockwave["start_location"] - me_shockwave["start_location"]) <= 50:
					if abs(gt_shockwave["end_time"] - me_shockwave["end_time"]) <= 30 and abs(gt_shockwave["end_x"] - me_shockwave["end_x"]) <= 50:
						gt_shockwave_output.append(gt_shockwave)
						me_shockwave_output.append(me_shockwave)
						shockwave_found = True

			if shockwave_found:
				count = count + 1
			else:
				out_f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (str(gt_shockwave['time']), str(gt_shockwave['start_location']), str(gt_shockwave['shockwave_length']), str(gt_shockwave['end_time']), str(gt_shockwave['end_x']), str(gt_shockwave['shockwave_type']), str(gt_shockwave['shockwave_propogation_speed']), str(gt_shockwave['shockwave_count']), str(gt_shockwave['significant_shockwave'])))


print count
with open(gt_shockwave_output_file, "wb") as out_f:
	for gt_shockwave in gt_shockwave_output:
		out_f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (str(gt_shockwave['time']), str(gt_shockwave['start_location']), str(gt_shockwave['shockwave_length']), str(gt_shockwave['end_time']), str(gt_shockwave['end_x']), str(gt_shockwave['shockwave_type']), str(gt_shockwave['shockwave_propogation_speed']), str(gt_shockwave['shockwave_count']), str(gt_shockwave['significant_shockwave'])))

with open(me_shockwave_output_file, "wb") as out_f:
	for me_shockwave in me_shockwave_output:
		out_f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (str(me_shockwave['time']), str(me_shockwave['start_location']), str(me_shockwave['shockwave_length']), str(me_shockwave['end_time']), str(me_shockwave['end_x']), str(me_shockwave['shockwave_type']), str(me_shockwave['shockwave_propogation_speed']), str(me_shockwave['shockwave_count']), str(me_shockwave['significant_shockwave'])))