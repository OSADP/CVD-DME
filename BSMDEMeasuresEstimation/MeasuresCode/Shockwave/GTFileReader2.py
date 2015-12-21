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
# from superlinks_VanNess import vanness_links, intersection_links

t = Timer(enabled=True)



class Trajectories(object):
    """Core class for reading vehicles trajectories"""

    def __init__(self, filename, tp=3):

        self.filename = filename
        self.tp_loc = tp



    def read_line(self, line, header=False):
        if header:
            return [x.strip() for x in line.split(';')][:-1]
        else:
            data = line.split(';')[:-1]
            if len(data)>0:
                return {
                      'vehid': float(data[0].strip()),
                      'a': float(data[1].strip()) * 0.3048, # convert ft/s^2 to m/s^2
                      'v': float(data[2].strip()) * 5280 / 3600, # convert mph to ft/s
                      'tp': float(data[3].strip()),
                      'Link': float(data[4].strip()),
                      'Lane': float(data[5].strip()),
                      'x': float(data[6].strip()),
                      'Length': float(data[7].strip()),
                      'Link_x' : float(data[6].strip()),
                      'World_x': float(data[8].strip()) * 100 / 2.54 / 12 ,
                      'World_y': float(data[9].strip()) * 100 / 2.54 / 12 ,
                }
            else:
                print line
                return None



    def read_line_int(self, line, header=False):
        if header:
            return [x.strip() for x in line.split(';')][:-1]
        else:
            data = line.split(';')[:-1]
            if len(data)>0:
                return {
                      'vehid': float(data[0].strip()),
                      'a': float(data[1].strip()) * 0.3048, # convert to ft/s^2 to m/s^2
                      'v': float(data[2].strip()) * 5280 / 3600, # convert mph to ft/s
                      'tp': float(data[3].strip()),
                      'Link': float(data[5].strip()),
                      'Lane': float(data[4].strip()),
                      'x': float(data[6].strip()),
                      'Length': float(data[7].strip()),
                      'Link_x' : float(data[6].strip()),
                }
            else:
                print line
                return None



    def read(self):

        # c=0

        # with open(self.filename) as in_f:
        #     line = in_f.readline()
        #     while 'VehNr;' not in line:
        #         line = in_f.readline()

        #     header =  self.read_line(line, header=True)
        #     # print header

        #     line = self.read_line(in_f.readline())
        #     old_tp = None
        #     tp_list = []

        #     while line:

        #         tp = line['tp']
        #         if tp != old_tp:

        #             if old_tp != None:
        #                 yield old_tp, tp_list

        #             old_tp = tp
        #             tp_list = []


        #         tp_list.append(line)
        #         line = self.read_line(in_f.readline())
        #         c +=1

        #         # if c % 50000 ==0:
        #         #     print 'Read %s lines' % (str(c))

        #     #yield last tp
        #     yield tp, tp_list
        flag = True
        old_tp = None
        tp_list = []

        with open(self.filename) as in_f:
          for l in in_f:
            if flag and 'VehNr;' not in l:
              continue
            elif flag:
              flag = False
              continue
            line = self.read_line(l)
            tp = line['tp']
            if tp != old_tp:

                if old_tp != None:
                    yield old_tp, tp_list   
                old_tp = tp
                tp_list = []

            tp_list.append(line)
          #yield last tp
          yield tp, tp_list



class BSMs(object):
    """Core class for reading vehicles trajectories"""

    def __init__(self, filename, tp=10):

        self.filename = filename
        self.tp_loc = tp



    def read_line(self, line, header=False):
        if header:
            return [x.strip() for x in line.split(',')][:-1]
        else:
            data = line.split(',')[:]
            if len(data)>0:
                return {
                      'a': float(data[6].strip()) * 0.3048,
                      'v': float(data[11].strip()) * 5280 / 3600, # convert mph to ft/s,
                      'tp': float(data[10].strip()),
                      'Link': float(data[8].strip()),
                      'Lane': float(data[7].strip()),
                      'x': float(data[9].strip()),
                      'Link_x' : float(data[9].strip()),
                      'World_x': float(data[15].strip()),
                      'World_y': float(data[16].strip()),
                }
            else:
                print line
                return None


    def read(self):

        flag = True
        old_tp = None
        tp_list = []

        with open(self.filename) as in_f:
          for l in in_f:
            if flag:
              flag = False
              continue
            line = self.read_line(l)
            tp = line['tp']
            if tp != old_tp:

                if old_tp != None:
                    yield old_tp, tp_list   
                old_tp = tp
                tp_list = []

            tp_list.append(line)
          #yield last tp
          yield tp, tp_list


class PDMs(object):
    """Core class for reading vehicles trajectories"""

    def __init__(self, filename, tp=10):

        self.filename = filename
        self.tp_loc = tp



    def read_line(self, line, header=False):
        if header:
            return [x.strip() for x in line.split(',')][:-1]
        else:
            data = line.split(',')[:]
            if len(data)>0:
                return {
                      'a': float(data[8].strip()) * 0.3048,
                      'v': float(data[2].strip()) * 5280 / 3600, # convert mph to ft/s,
                      'tp': float(data[0].strip()),
                      'Link': float(data[5].strip()),
                      'Lane': float(data[6].strip()),
                      'x': float(data[7].strip()),
                      'Link_x' : float(data[9].strip()),
                      'World_x': float(data[3].strip()),
                      'World_y': float(data[4].strip()),
                }
            else:
                print line
                return None


    def read(self):

        flag = True
        old_tp = None
        tp_list = []

        with open(self.filename) as in_f:
          for l in in_f:
            if flag:
              flag = False
              continue
            line = self.read_line(l)
            tp = line['tp']
            if tp != old_tp:

                if old_tp != None:
                    yield old_tp, tp_list   
                old_tp = tp
                tp_list = []

            tp_list.append(line)
          #yield last tp
          yield tp, tp_list





#
#
#
# # TESTING
# filename = r'D:\Data\Tasks\FHWA\Current\DCM_Contract\BSM Emulator\GT_Coding\VISSIM_files\VanNess\2005_pm_nb_calibrated_notransit3_medDemand_Incident.fzp'
# trj = Trajectories(filename, 3)
#
# super_links, all_key_links = vanness_links()
# #
# #
# # c =0
# # len_old = []
# # t.start('old')
# # for tp, df in trj.read():
# #     stop_df = df[ (df['v'] == 0)
# #              & (df['Link'].isin(all_key_links))]
# #
# #     c+=1
# #     if c== 50000:
# #         break
# #
# #     len_old.append(len(stop_df))
# #
# #
# # t.stop('old')
# # print t['old']
# #
# c =0
# len_new = []
# t.start('new')
# for tp, df in trj.read2():
#
#     print df
#     ddd
#
#     new = []
#     for row in df:
#         if row['v'] == 0:
#             if row['Link'] in all_key_links:
#                 new.append(row)
#     c+=1
#     if c== 50000:
#         break
#
#     len_new.append(len(new))
#
#
# t.stop('new')
#
#
# print t['new']
# print len_old == len_new
# print len(len_new)

#*************************************************************************
# class Trajectories_Tests(unittest.TestCase):

#     def setUp(self):
#         pass



#     # @unittest.skip("testing skipping")
#     def test_load_read_csv(self):

#         filename = r'D:\Data\Tasks\FHWA\Current\DCM_Contract\BSM Emulator\GT_Coding\VISSIM_files\VanNess\2005_pm_nb_calibrated_notransit3_highDemand_NoIncident.fzp'
#         trj = Trajectories(filename)

#         trj.load()





#     def tearDown(self):
#         pass


# if __name__ == '__main__':
#     unittest.main()
