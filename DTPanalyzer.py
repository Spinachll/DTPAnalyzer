import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import re
import collections 

class UploadFromCSV:

    months = ['январь','февраль','март',
              'апрель','май','июнь',
              'июль','август','сентябрь',
              'октябрь','ноябрь','декабрь']
    
    """#Преобразовывает Frame в два массива """
    @staticmethod    
    def ConvToArrays(my_dict): 
        keys = []
        values = []
        for k, v in my_dict.iterrows():
            keys.append(v['S'])
            values.append(v['N'])
        return (keys,values)

    def __init__(self, fileName):
        self.fileName = fileName
        self.datas = pd.DataFrame()
        self.masBack = pd.DataFrame()
        self.dataBack = pd.DataFrame() 

    def readFile(self, low_memory):
        try:
            self.datas = pd.read_csv(self.fileName, low_memory=low_memory)
        except Exception as e:
            print(e)
        return self.datas

    def readFile(self, low_memory, fileName):
        try:
            self.datas = pd.read_csv(fileName, low_memory=low_memory)
        except Exception as e:
            if (str(e).find('Error tokenizing data') != -1):
                #print(e)
                self.datas = pd.read_csv(fileName, low_memory=low_memory, delimiter=';')
        return self.datas
    
    def readFile(self, low_memory, fileName, sep):
        try:
            self.datas = pd.read_csv(fileName, low_memory=low_memory, sep=sep)
        except Exception as e:
            if (str(e).find('Error tokenizing data') != -1):
                #print(e)
                self.datas = pd.read_csv(fileName, low_memory=low_memory, delimiter=';')
        return self.datas

    def parseFilesDTPGood(self):
        path = 'D:\Python\Projects\Datasets\DTP_fixed\All'
        num_files = len( [f for f in os.listdir(path)
                         if os.path.isfile(os.path.join(path, f))])
        mas = pd.DataFrame()
        masPast = pd.DataFrame()
        self.masBack = pd.DataFrame()
        yearPast = 0;
        year = 0;
        for fName in os.listdir(path):
            masResult = pd.DataFrame()
            mas = pd.DataFrame()
            mastmp = self.readFile(False, path + '\\' + fName)
            tmp_count = 0
            tmpData = list(mastmp)[3].split('-')
            if (len(tmpData) == 1):
                month = re.findall(r'(январь)', tmpData[0])
            elif (len(tmpData) == 2):
                month = re.findall(r'(январь|февраль|март|апрель|май|июнь|июль|август|сентябрь|октябрь|ноябрь|декабрь)', tmpData[1])
                
            year =  re.findall(r'20[0-9][0-9]',list(mastmp)[3])   
            data = ''.join(month)+' '+''.join(year)
            
            for index, row in mastmp.iterrows():
                if (row[2].find('Количество ДТП с пострадавшими') != -1):
                    df1 = pd.DataFrame([[row[list(mastmp)[0]], data,
                                         row[list(mastmp)[3]],'1','1']],
                                       columns = ['Субъект', 'Дата',
                                                  'Количество ДТП с пострадавшими',
                                                  'Количество лиц, погибших в результате ДТП',
                                                  'Количество лиц, получивших ранения в результате совершения ДТП'])
                    
                    mas = pd.concat([mas, df1])
                    
                elif (row[2].find('Количество лиц, погибших в результате ДТП') != -1):
                    for i, j in mas.iterrows():
                        if j[0] == row[list(mastmp)[0]]:
                            j[3] = row[list(mastmp)[3]]
                            break
                elif (row[2].find('Количество лиц, получивших ранения в результате совершения ДТП') != -1):
                    for i, j in mas.iterrows():
                        if j[0] == row[list(mastmp)[0]]:
                            j[4] = row[list(mastmp)[3]]
                            break

            if yearPast != 0:
                if year == yearPast:
                    #Вычитание masPast из mas
                    for i, j in mas.iterrows():
                        for k, m in masPast.iterrows():
                            if (j[0] == m[0]) and (re.findall(r'20[0-9][0-9]',j[1]) == re.findall(r'20[0-9][0-9]',m[1])):
                                dfTemp = pd.DataFrame([[j[0], data,
                                                       str(float(j[2]) - float(m[2])),
                                                       str(float(j[3]) - float(m[3])),
                                                       str(float(j[4]) - float(m[4]))]],
                                                      columns = ['Субъект', 'Дата',
                                                                 'Количество ДТП с пострадавшими',
                                                                 'Количество лиц, погибших в результате ДТП',
                                                                 'Количество лиц, получивших ранения в результате совершения ДТП'])
                                masResult = pd.concat([masResult, dfTemp])
                                break
                else:
                    masResult = pd.concat([masResult, mas])
                
                    
            masPast = pd.DataFrame()
            masPast = pd.concat([masPast, mas])
            self.masBack = pd.concat([self.masBack, masResult])
            yearPast = year
            #print(data);
            #print(masResult)
        return self.masBack
            

    

    def parseFileFedRoads(self):
        path = 'D:\Python\Projects\Datasets\Roads'
        fileName = 'Fed_roads.csv'

        masTmp = self.readFile(False, path + '\\' + fileName)
        #print(masTmp)
        #for index, row in masTmp.iterrows():
        #   print(row)
        return masTmp


    def parseFileRegRoads(self):
        path = 'D:\Python\Projects\Datasets\Roads'
        fileName = 'Reg_roads.csv'

        masTmp = self.readFile(False, path + '\\' + fileName)
        #print(masTmp)
        #for index, row in masTmp.iterrows():
        #   print(row)
        return masTmp

    def parseFileAvgAge(self):
        path = 'D:\Python\Projects\Datasets\AVG_age'
        fileName = 'AVG_age.csv'

        masTmp = self.readFile(False, path + '\\' + fileName, ';')
        #print(masTmp)
        #for index, row in masTmp.iterrows():
        #   print(row)
        return masTmp



    def parseFileMeds(self):
        path = 'D:\Python\Projects\Datasets\MEDS'
        fileName = 'Meds.csv'

        readData = self.readFile(False, path + '\\' + fileName, ',')
        masTmp = pd.DataFrame()

        for index, row in readData.iterrows():
            df1 = pd.DataFrame([[row['reg_code'], row['reg_name'],
                                         row['medical_type']]],
                                columns = ['reg_code', 'reg_name',
                                           'medical_type'])
            masTmp = pd.concat([masTmp, df1])
        print(masTmp)
        return masTmp
    
        #print(masTmp)
        #for index, row in masTmp.iterrows():
        #   print(row)
        #print(masTmp)
        #c = collections.Counter()
        #for index, row in masTmp.iterrows():
        #    c[row['medical_type']] += 1
        #print(list(c))
        


    def parseFileCargo(self):
        path = 'D:\Python\Projects\Datasets\Cargo'
        fileName = 'Cargo.csv'

        masTmp = self.readFile(False, path + '\\' + fileName, ';')
        #print(masTmp)
        #for index, row in masTmp.iterrows():
        #   print(row)
        return masTmp
















































            
