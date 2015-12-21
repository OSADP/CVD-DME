#standard
import os
import random as rnd
import sys
import tempfile as tmpfile
import unittest
import logging

#external
import pandas as pd


from TCACore import Timer

t = Timer(enabled=True)
CONVERSION_METERS_TO_FEET = 100 / 2.54 / 12


class Messages(object):
    """Core class for reading vehicles BSMs and PDMs"""

    def __init__(self, filename, tp=3):

        self.filename = filename
        self.tp_loc = tp


    def read_line_PDM(self, line, header=False):
        if header:
            return [x.strip() for x in line.split(',')]
        else:
            data = line.split(',')
            if len(data) >= 7:
                return {
                      'v': float(data[2].strip()) * 5280 / 3600, # convert mph to ft/s
                      'tp': float(data[0].strip()),
                      'PSN': float(data[1].strip()),
                      'a': float(data[8].strip()),
                      'x': float(data[3].strip()),
                      'y': float(data[4].strip()),
                      'link': int(data[5].strip()),
                      'lane': int(data[6].strip()),
                }
            else:
                print line
                return None

    def read_line(self, line, header=False):
        if header:
            return [x.strip() for x in line.split(',')]
        else:
            data = line.split(',')
            if len(data) >= 5:
                return {
                      'v': float(data[3].strip()) * 5280 / 3600, # convert mph to ft/s
                      'tp': float(data[2].strip()),
                      'a': float(data[0].strip()),
                      'x': float(data[4].strip()),
                      'y': float(data[5].strip()),
                      'link': int(data[1].strip()),
                }
            else:
                print line
                return None

    def read(self):

        c=0

        # with open(self.filename) as in_f:
        #     t.start('sort_BSMs')
        #     df = pd.read_csv(in_f, index_col='PSN')
        #     df.sort_index(by='localtime', inplace = True)
        #     t.stop('sort_BSMs')
        #     df.to_csv(self.filename + '2.csv')


        with open(self.filename) as in_f: # + '2.csv') as in_f:
            # If a PDM file
            if 'PDM' in self.filename:

                line = in_f.readline()

                header =  self.read_line_PDM(line, header=True)

                line = self.read_line_PDM(in_f.readline())
                old_tp = None
                tp_list = []

                while line:

                    tp = line['tp']
                    if tp != old_tp:

                        if old_tp != None:
                            yield old_tp, tp_list

                        old_tp = tp
                        tp_list = []


                    tp_list.append(line)
                    line = self.read_line_PDM(in_f.readline())
                    c +=1

                    # if c % 50000 ==0:
                    #     print 'Read %s lines' % (str(c))

                #yield last tp
                yield tp, tp_list

            # It's a BSM file
            else:
                line = in_f.readline()

                header =  self.read_line(line, header=True)

                line = self.read_line(in_f.readline())
                old_tp = None
                tp_list = []

                while line:

                    tp = line['tp']
                    if tp != old_tp:

                        if old_tp != None:
                            yield old_tp, tp_list

                        old_tp = tp
                        tp_list = []


                    tp_list.append(line)
                    line = self.read_line(in_f.readline())
                    c +=1

                    # if c % 50000 ==0:
                    #     print 'Read %s lines' % (str(c))

                #yield last tp
                yield tp, tp_list

    def read_route(filename):
        # Route file data: Route name, link order num, link #, X coord, Y coord
        #                  Route name, link order num, link #, X coord, Y coord
        
        routes = {}

        for line in open(filename):

            row = line.strip().split(',')
            route_name = row[1]
            direction = row[0]
            heading = float(row[2])
            stopline_x = float(row[3]) * CONVERSION_METERS_TO_FEET 
            stopline_y = float(row[4]) * CONVERSION_METERS_TO_FEET 

            if direction not in stopline_positions.keys():
                stopline_positions[direction] = {}

            if known_queue not in stopline_positions[direction].keys():
                stopline_positions[direction][known_queue] = {'heading' : heading, 'x' : stopline_x, 'y': stopline_y}

        return routes

#*************************************************************************
class Messages_Tests(unittest.TestCase):

    def setUp(self):
        pass

    # @unittest.skip("testing skipping")
    def test_load_read_csv(self):

        filename = r'C:\Users\M29565\Documents\Projects\tca\Measures_code\Queue\intersection_BSMs.csv'
        msg = Messages(filename)

        for tp, tp_list in msg.read():
            print tp
            print len(tp_list)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
