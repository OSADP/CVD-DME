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


class Messages(object):
    """Core class for reading vehicles BSMs and PDMs"""

    def __init__(self, filename, link_dict, interpolate = False):

        self.filename = filename
        self.df = None
        if interpolate:
            filepath_split = self.filename.split('/')
            filename_split = filepath_split[-1].split('.')
            dir_path = os.path.dirname( os.path.realpath( __file__ ) )
            out_PDM_file = dir_path + '/Interpolated_PDM_Files/' + filename_split[0] + '_Interpolated.csv'
            with open(out_PDM_file, 'wb') as out_f:
                out_f.write("Time_Taken,PSN,Speed,X,Y,Link,Lane,Link_x,Acceleration,Type,Transmit_To,Transmit_Time\n")

            # Put all the PDMs in a DataFrame
            df = pd.read_csv(filepath_or_buffer=self.filename,header=0,skipinitialspace = True, \
                usecols=['Time_Taken','PSN','Speed','X','Y','Link','Lane','Link_x','Acceleration','Type','Transmit_To','Transmit_Time'])

            # Retrieve all the PDMs generated on a Van Ness NB superlink
            df['onSuperlinks'] = df.apply(lambda row: self.on_superlinks(row['Link'], link_dict), axis = 1)
            df_valid_pdms = df[df['onSuperlinks'] == 1]

            # For each unique PSN, retrieve all the PDMs associated with that PSN
            unique_PSNs = df_valid_pdms['PSN'].unique()
            for PSN in unique_PSNs:
                df_psn = df_valid_pdms[df_valid_pdms['PSN'] == PSN]
                unique_transmit_times = df_psn['Transmit_Time'].unique()

                # For each Transmit Time, retrieve all the PDMs associated with that PSN/Travel_Time combination
                for tt in unique_transmit_times:
                    df_psn_tt = df_psn[df_psn['Transmit_Time'] == tt]
                    unique_RSE_list = df_psn_tt['Transmit_To'].unique()

                    for RSE in unique_RSE_list:
                        df_psn_tt_RSE = df_psn_tt[df_psn_tt['Transmit_To'] == RSE]

                        # Interpolate to get the new PDMs
                        last_row = len(df_psn_tt_RSE.index) - 1
                        df_psn_indexed = df_psn_tt_RSE.set_index('Time_Taken')
                        print "Processing...:"
                        print PSN
                        print tt
                        print RSE
                        df_psn_full = df_psn_indexed.reindex(list(range(int(df_psn_tt_RSE.iat[0,0]),int(df_psn_tt_RSE.iat[last_row,0])+1)))
                        df_psn_full['Link'] = df_psn_full['Link'].ffill()
                        df_psn_full['Lane'] = df_psn_full['Lane'].ffill()
                        df_psn_full['Type'] = df_psn_full['Type'].ffill()
                        df_psn_full['onSuperlinks'] = df_psn_full['onSuperlinks'].ffill()
                        df_final = df_psn_full.apply(pd.Series.interpolate)
                        
                        col = ['PSN','Speed','X','Y','Link','Lane','Link_x','Acceleration','Type','Transmit_To','Transmit_Time']
                        df_final_formatted = pd.DataFrame(df_final, columns = col)
                        df_final_formatted['Link'] = df_final_formatted['Link'].map('{:.0f}'.format)
                        df_final_formatted['Lane'] = df_final_formatted['Lane'].map('{:.0f}'.format)
                        df_final_formatted['Type'] = df_final_formatted['Type'].map('{:.0f}'.format)
                        df_final_formatted['Acceleration'] = df_final_formatted['Acceleration'].map('{:.3f}'.format)
                        df_final_formatted['Link_x'] = df_final_formatted['Link_x'].map('{:.2f}'.format)
                        
                        df_final_formatted.to_csv(out_PDM_file, mode='a', header=False)

            df = pd.read_csv(out_PDM_file)
            df_sorted = df.sort('Time_Taken')
            df_sorted.to_csv(out_PDM_file,index=False)
            self.filename = out_PDM_file

    def on_superlinks(self, link, link_dict):
        if link in link_dict.keys():
            return 1

        else:
            return 0

    def read_line_PDM(self, line, header=False):
        if header:
            return [x.strip() for x in line.split(',')]
        else:
            data = line.split(',')
            if len(data) >= 7:
                return {
                      'v': float(data[2].strip()) * 5280 / 3600, # convert mph to ft/s
                      'tp': float(data[0].strip()),
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
            if len(data) >= 16:
                return {
                      'v': float(data[11].strip()) * 5280 / 3600, # convert mph to ft/s
                      'tp': float(data[10].strip()),
                      'heading': str(data[5].strip()),
                      'a': float(data[6].strip()),
                      'x': float(data[15].strip()),
                      'y': float(data[16].strip()),
                      'link': int(data[8].strip()),
                      'lane': int(data[7].strip()),
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
