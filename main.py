import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from DTPanalyzer import UploadFromCSV      
from SQLiteInspector import SQLiteInspector
import collections
import cufflinks as cf
import sqlite3

def main():
    
    tempDB = SQLiteInspector('testDB1')
    #tempDB.alterTable('ALTER TABLE tableReg RENAME TO tableFed;')
    
    
    
    tmpDTP = UploadFromCSV("noname")
    '''
    tempDB.createTable('CREATE TABLE IF NOT EXISTS tableArea (Region TEXT,'+
                       'area real,'+                                        
                       'PRIMARY KEY(Region));');

    tempDB.createTable('CREATE TABLE tableFed (Region TEXT,'+
                       'y2007 integer, y2008 integer,'+
                       'y2009 integer, y2010 integer,'+
                       'y2011 integer, y2012 integer,'+
                       'y2013 integer, y2014 integer,'+
                       'y2015 integer, y2016 integer,'+
                       'y2017 integer, y2018 integer,'+                       
                       'PRIMARY KEY(Region));');
                       
    tempDB.createTable('CREATE TABLE tableReg (Region TEXT,'+
                       'y2007 integer, y2008 integer,'+
                       'y2009 integer, y2010 integer,'+
                       'y2011 integer, y2012 integer,'+
                       'y2013 integer, y2014 integer,'+
                       'y2015 integer, y2016 integer,'+
                       'y2017 integer, y2018 integer,'+                       
                       'PRIMARY KEY(Region));');
    
    tempDB.createTable('CREATE TABLE tableDTP (Region TEXT,'+
                       'date TEXT, NumDTP real,'+
                       'DeathDTP real, InjDTP real,'+ 
                       'PRIMARY KEY(Region, date));');

    tempDB.createTable('CREATE TABLE AVGage (Region TEXT,'+
                       'NumCars real,'+
                       'age real,'+ 
                       'PRIMARY KEY(Region));');

    tempDB.createTable('CREATE TABLE IF NOT EXISTS Meds' +
                       '(idNum integer PRIMARY KEY AUTOINCREMENT NOT NULL,'+
                       'numReg integer,'+
                       'Region TEXT,'+
                       'medType TEXT);');                       

    tempDB.createTable('CREATE TABLE tableCargo (Region TEXT,'+
                       'y2000 integer,'+
                       'y2001 integer, y2002 integer,'+
                       'y2003 integer, y2004 integer,'+
                       'y2005 integer, y2006 integer,'+
                       'y2007 integer, y2008 integer,'+
                       'y2009 integer, y2010 integer,'+
                       'y2011 integer, y2012 integer,'+
                       'y2013 integer, y2014 integer,'+
                       'y2015 integer, y2016 integer,'+
                       'y2017 integer, y2018 integer,'+                       
                       'PRIMARY KEY(Region));');

    tempDB.createTable('CREATE TABLE IF NOT EXISTS tableMedsCount (Region TEXT,'+
                       'NumMeds integer,'+
                       'PRIMARY KEY(Region));');
    
    
    tmpDTP = UploadFromCSV("noname")
    masTmp = tmpDTP.parseFilesDTPGood()
   
    for index, row in masTmp.iterrows():
        tempDB.insertValue('INSERT INTO tableDTP (Region, date, NumDTP, DeathDTP, InjDTP) VALUES("' +
                           row['Субъект'] + '","' + 
                           row['Дата'] + '","' + row['Количество ДТП с пострадавшими'] + '","' +
                           row['Количество лиц, погибших в результате ДТП'] + '","' +
                           row['Количество лиц, получивших ранения в результате совершения ДТП'] +
                           '");')
    
    masTmp = tmpDTP.parseFileFedRoads()
         
    for index, row in masTmp.iterrows():
        tempDB.insertValue('INSERT INTO tableFed (Region, y2007, y2008, y2009, y2010, y2011, y2012, y2013, y2014, y2015, y2016, y2017, y2018) VALUES("' +
                           row['Округ'] + '","' + 
                           row['2007'] + '","' + row['2008'] + '","' +
                           row['2009'] + '","' + row['2010'] + '","' +
                           row['2011'] + '","' + row['2012'] + '","' +
                           row['2013'] + '","' + row['2014'] + '","' +
                           row['2015'] + '","' + row['2016'] + '","' +
                           row['2017'] + '","' + row['2018'] + '");')
   
    for row in tempDB.selectValues('SELECT * FROM tableFed'):
        print(row)
    
    
    masTmp = tmpDTP.parseFileRegRoads()
    for index, row in masTmp.iterrows():
        tempDB.insertValue('INSERT INTO tableReg (Region, y2007, y2008, y2009, y2010, y2011, y2012, y2013, y2014, y2015, y2016, y2017, y2018) VALUES("' +
                           row['Округ'] + '","' + 
                           row['2007'] + '","' + row['2008'] + '","' +
                           row['2009'] + '","' + row['2010'] + '","' +
                           row['2011'] + '","' + row['2012'] + '","' +
                           row['2013'] + '","' + row['2014'] + '","' +
                           row['2015'] + '","' + row['2016'] + '","' +
                           row['2017'] + '","' + row['2018'] + '");')
    for row in tempDB.selectValues('SELECT * FROM tableReg'):
        print(row)
    
    
    masTmp = tmpDTP.parseFileAvgAge()
    print(masTmp)
    for index, row in masTmp.iterrows():
        tempDB.insertValue('INSERT INTO AVGage (Region, NumCars, age) VALUES("' +
                           row['Округ'] + '","' + 
                           row['ОбъемРынка'] + '","' + row['СреднийВозраст'] + '");')
    for row in tempDB.selectValues('SELECT * FROM AVGage'):
        print(row)
    

    masTmp = tmpDTP.parseFileMeds()
    for index, row in masTmp.iterrows():
        tempDB.insertValue('INSERT INTO Meds (numReg, Region, medType) VALUES("' +
                           str(row['reg_code']) + '","' + 
                           str(row['reg_name']) + '","' + str(row['medical_type']) + '");')
    for row in tempDB.selectValues('SELECT * FROM Meds'):
        print(row)
    

    
    
    masTmp = tmpDTP.parseFileCargo()
    for index, row in masTmp.iterrows():
        tempDB.insertValue('INSERT INTO tableCargo (Region, y2000,'+
                           'y2001, y2002, y2003, y2004, y2005, y2006,'+
                           'y2007, y2008, y2009, y2010, y2011, y2012,'+
                           'y2013, y2014, y2015, y2016, y2017, y2018) VALUES("' +
                           row['Округ'] + '","' + row['2000'] + '","' +                            
                           row['2001'] + '","' + row['2002'] + '","' +
                           row['2003'] + '","' + row['2004'] + '","' +
                           row['2005'] + '","' + row['2006'] + '","' +
                           row['2007'] + '","' + row['2008'] + '","' +
                           row['2009'] + '","' + row['2010'] + '","' +
                           row['2011'] + '","' + row['2012'] + '","' +
                           row['2013'] + '","' + row['2014'] + '","' +
                           row['2015'] + '","' + row['2016'] + '","' +
                           row['2017'] + '","' + row['2018'] + '");')
    
    for row in tempDB.selectValues('SELECT * FROM tableCargo'):
        print(row)
    

    masTmp = tempDB.selectValues('SELECT * FROM Meds')
    c = collections.Counter()
    for row in masTmp:
        c[row[2]] += 1

    for row in c:
        print(str(row) +' '+ str(c[row]))
        tempDB.insertValue('INSERT INTO tableMedsCount (Region, NumMeds) VALUES("' +
                           str(row) + '",' + 
                           str(c[row]) + ');')
    for row in tempDB.selectValues('SELECT * FROM tableMedsCount'):
       print(row)

    '''


    #masTmp = tempDB.selectValues('SELECT * FROM tableCargo')
    #Вывод графика Начало
    '''
    cnx = sqlite3.connect('testDB1')
    masTmp = pd.read_sql_query('SELECT Region as Region,y2000 as "2000",'+
                               'y2001 as "2001", y2002 as "2002", y2003 as "2003",'+
                               'y2004 as "2004", y2005 as "2005", y2006 as "2006",'+
                               'y2007 as "2007", y2008 as "2008", y2009 as "2009",'+
                               'y2010 as "2010", y2011 as "2011", y2012 as "2012",'+
                               'y2013 as "2013", y2014 as "2014", y2015 as "2015",'+
                               'y2016 as "2016", y2017 as "2017", y2018 as "2018"'+
                               ' FROM tableCargo', cnx)
    print('VIVOD1') 
    
    #masTmp.replace(',','.')
    
    for i,row in masTmp.iterrows():
        for col in masTmp.columns:
            masTmp.loc[i][col] = str(masTmp.loc[i][col]).replace(',','.')

    
            
    print('VIVOD2') 
    masRegion = masTmp['Region'].to_list()
    masTmp = masTmp.drop(masTmp.columns[0], axis = 'columns')
    masTmp = masTmp.T
    masTmp.columns = masRegion
    print(masTmp)
    
    fig = masTmp.iplot(asFigure=True, xTitle="Year",
                       yTitle="tonns, mln", title="Cargo")
    fig.show()

    #tempDB.alterTable('ALTER TABLE tableDTP ADD Column year integer;')
    #for i, row in masTmp.iterrows():
    #    masTmp.loc[i,'year'] = int(row['date'].split(' ')[1])
    #   tempDB.updateValue('UPDATE tableDTP SET year = "' + str(masTmp.loc[i,'year'])+
    #                   '" WHERE Region = "' + row['Region'] +'" AND date = "'+ str(row['date']) + '";')
    '''
    #Вывод графика Конец

    
    #Вывод Возраста Автомобилей
    cnx = sqlite3.connect('testDB1')
    masAge = pd.read_sql_query('SELECT Region as Region, age as "age"'+                               
                               ' FROM AVGage', cnx)
    for i,row in masAge.iterrows():
        masAge.loc[i]['age'] = float(str(masAge.loc[i]['age']).replace(',','.'))
    minNum = masAge.min()['age']
    maxNum = masAge.max()['age'] - minNum
    masAge['age'] = (masAge['age'] - minNum)/maxNum
    #Замена числовых индексов на Названия регионов
    #Закомментировать, если не нужно
    #print(masAge)
    masRegions = masAge['Region'].to_list()
    masAge = masAge.drop(masAge.columns[0], axis = 'columns')
    masAge.index = masRegions
    print(masAge)
    print(masRegions)

    #Вывод площади региона
    masArea = pd.read_sql_query('SELECT Region as Region, area as "area"'+                               
                               ' FROM tableArea', cnx)
    #print(masArea)
    tmpArea = masArea['Region'].to_list()
    masArea = masArea.drop(masArea.columns[0], axis = 'columns')
    masArea.index = tmpArea
    print(masArea)

    #Вывод Количества Медучреждений
    masMeds = pd.read_sql_query('SELECT Region as Region, NumMeds as "numMeds"'+
                                ' FROM tableMedsCount', cnx)
    
    #masMeds['numMeds'] = masMeds['numMeds'].astype('float')

    print(masMeds)
    masMeds['numMeds'] = masMeds['numMeds'].astype('float')
    print('VIVOD')
    for row in masMeds.index:
        if (masMeds.loc[row]['Region'] in masArea.index):
            #print(masArea.loc[masMeds.loc[row]['Region']]['area'])
            #print(masMeds.loc[row]['numMeds']/ masArea.loc[masMeds.loc[row]['Region']]['area'])
            masMeds.at[row, 'numMeds'] = float(masMeds.at[row,'numMeds'])/ masArea.at[masMeds.at[row, 'Region'], 'area']
            
    print(masMeds)
            
    minNum = masMeds.min()['numMeds']
    maxNum = masMeds.max()['numMeds'] - minNum
    masMeds['numMeds'] = (masMeds['numMeds'] - minNum)/maxNum
    #Замена числовых индексов на Названия регионов
    #Закомментировать, если не нужно
    #print(masMeds)
    masTmp = masMeds['Region'].to_list()
    masMeds = masMeds.drop(masMeds.columns[0], axis = 'columns')
    masMeds.index = masTmp
    print(masMeds)
    

    #Вывод Количества Перевезенных Грузов
    masCargo = pd.read_sql_query('SELECT *'+
                                ' FROM tableCargo', cnx)

    print(masCargo)
    for i,row in masCargo.iterrows():
        print(row['Region'])
        for j in masCargo.columns:
            if j.find('20') != -1:
                masCargo.at[i,j] = float(str(masCargo.at[i,j]).replace(',','.'))
                if row['Region'] in masArea.index:
                    masCargo.at[i,j] = masCargo.at[i,j]/masArea.at[row['Region'], 'area']
                


    print(masCargo)
    '''
    masCargo['numMeds'] = masCargo['numMeds'].astype('float')
    print('VIVOD')
    for row in masCargo.index:
        if (masCargo.loc[row]['Region'] in masArea.index):
            #print(masArea.loc[masMeds.loc[row]['Region']]['area'])
            #print(masMeds.loc[row]['numMeds']/ masArea.loc[masMeds.loc[row]['Region']]['area'])
            masCargo.at[row, 'numMeds'] = float(masCargo.at[row,'numMeds'])/ masArea.at[masCargo.at[row, 'Region'], 'area']
    '''
   
    masTmp = masCargo['Region'].to_list()
    masCargo = masCargo.drop(masCargo.columns[0], axis = 'columns')
    masCargo = masCargo.T
    masCargo.columns = masTmp
    for i,row in masCargo.iterrows():
        minNum = row.min()
        maxNum = row.max() - minNum
        masCargo.loc[i] = (row - minNum)/maxNum
    #Переименование годов
    masCargo.index = [item.replace("y", "") for item in masCargo.index.to_list()]
    print(masCargo)
    

    #Вывод Дорог Федерального Значения
    masFed = pd.read_sql_query('SELECT *'+
                               ' FROM tableFed', cnx)
    for i,row in masFed.iterrows():
        for j in masFed.columns:
            if j.find('20') != -1:
                masFed.loc[i][j] = float(str(masFed.loc[i][j]).replace(',','.'))
    masTmp = masFed['Region'].to_list()
    masFed = masFed.drop(masFed.columns[0], axis = 'columns')
    masFed = masFed.T
    masFed.columns = masTmp
    for i,row in masFed.iterrows():
        minNum = row.min()
        maxNum = row.max() - minNum
        masFed.loc[i] = (row - minNum)/maxNum
    #Переименование годов
    masFed.index = [item.replace("y", "") for item in masFed.index.to_list()]
    print(masFed)
    

    #Вывод Дорог Регионального Значения
    masReg = pd.read_sql_query('SELECT *'+
                               ' FROM tableReg', cnx)
    for i,row in masReg.iterrows():
        for j in masReg.columns:
            if j.find('20') != -1:
                masReg.loc[i][j] = float(str(masReg.loc[i][j]).replace(',','.'))
    masTmp = masReg['Region'].to_list()
    masReg = masReg.drop(masReg.columns[0], axis = 'columns')
    masReg = masReg.T
    masReg.columns = masTmp
    for i,row in masReg.iterrows():
        minNum = row.min()
        maxNum = row.max() - minNum
        masReg.loc[i] = (row - minNum)/maxNum
    #Переименование годов
    masReg.index = [item.replace("y", "") for item in masReg.index.to_list()]
    print(masReg)

    #Вывод коэффициента смертности
    masDeath = pd.read_sql_query('SELECT *'+
                               ' FROM tableDTPdeath', cnx)
    masTmp = masDeath['Region'].to_list()
    masDeath = masDeath.drop(masDeath.columns[0], axis = 'columns')
    masDeath = masDeath.T
    masDeath.columns = masTmp
    #Присвоение коэффициентов
    print('masDeath:')
    print(masDeath)
    for i,row in masDeath.iterrows():
        minNum = row.min()
        maxNum = row.max() - minNum
        masDeath.loc[i] = (row - minNum)/maxNum
    #Переименование годов
    masDeath.index = [item.replace("y", "") for item in masDeath.index.to_list()]
    print(masDeath)
    

    #Вывод коэффициента пострадавших
    masInj = pd.read_sql_query('SELECT *'+
                               ' FROM tableDTPinjured', cnx)
    masTmp = masInj['Region'].to_list()
    masInj = masInj.drop(masInj.columns[0], axis = 'columns')
    masInj = masInj.T
    masInj.columns = masTmp
    masInj.index = [item.replace("y", "") for item in masInj.index.to_list()]
    #Присвоение коэффициентов
    print('masInj:')
    masInj = masInj.astype('float')
    print(masInj.astype('float'))
    print(masInj)
    for i,row in masInj.iterrows():
        minNum = row.min()
        maxNum = row.max() - minNum
        masInj.loc[i] = (row - minNum)/maxNum
    #Переименование годов
    
    print(masInj)
    

    masRegions.sort()
    tmpList = list()
    for i in range(5,21):
        if i<10:
            tmpList.append('200' + str(i))
        else:
            tmpList.append('20' + str(i))
    print(tmpList)

            
            
    masAll = pd.DataFrame(columns = masRegions, index = tmpList)
    #masAll = 0
    for col in masAll.columns:
        for row in masAll.index:
            masAll.loc[row][col] = 0.0
            
    print(masAll)

    for col in masAll.columns:
        #print(col)
        
        for row in masAll.index:
            #print(row)
            
            if col in masAge.index:
                #print(masAge.loc[col]['age'])
                masAll.loc[row][col] -=  masAge.loc[col]['age']
                                
            if col in masMeds.index:
                #print(masMeds.loc[col]['numMeds'])
                masAll.loc[row][col] += masMeds.loc[col]['numMeds']
                
            if (row in masCargo.index) and (col in masCargo):
                #print(masCargo.loc[row][col])
                masAll.loc[row][col] -=  masCargo.loc[row][col]
            else:
                for i in range(1,10):
                    if (str((int(row)+i)) in masCargo.index) and (col in masCargo):
                        #print(masCargo.loc[str((int(row)+i))][col])
                        masAll.loc[row][col] -= masCargo.loc[str((int(row)+i))][col]
                        break
                    elif (str((int(row)-i)) in masCargo.index) and (col in masCargo):
                        #print(masCargo.loc[str((int(row)-i))][col])
                        masAll.loc[row][col] -= masCargo.loc[str((int(row)-i))][col]
                        break

            if (row in masFed.index) and (col in masFed):
                #print(masFed.loc[row][col])
                masAll.loc[row][col] -= masFed.loc[row][col]
            else:
                for i in range(1,10):
                    if (str((int(row)+i)) in masFed.index) and (col in masFed):
                        #print(masFed.loc[str((int(row)+i))][col])
                        masAll.loc[row][col] -= masFed.loc[str((int(row)+i))][col]
                        break
                    elif (str((int(row)-i)) in masFed.index) and (col in masFed):
                        #print(masFed.loc[str((int(row)-i))][col])
                        masAll.loc[row][col] -= masFed.loc[str((int(row)-i))][col]
                        break

            if (row in masReg.index) and (col in masReg):
                #print(masReg.loc[row][col])
                masAll.loc[row][col] -= masReg.loc[row][col]
            else:
                for i in range(1,10):
                    if (str((int(row)+i)) in masReg.index) and (col in masReg):
                        #print(masReg.loc[str((int(row)+i))][col])
                        masAll.loc[row][col] -= masReg.loc[str((int(row)+i))][col]
                        break
                    elif (str((int(row)-i)) in masReg.index) and (col in masReg):
                        #print(masReg.loc[str((int(row)-i))][col])
                        masAll.loc[row][col] -= masReg.loc[str((int(row)-i))][col]
                        break
    print(masAll)
    
    fig = masAll.iplot(asFigure=True, xTitle="Year",
                       yTitle="Coef", title="Coef")
    fig.show()
    fig = masDeath.iplot(asFigure=True, xTitle="Death",
                       yTitle="tonns, mln", title="Cargo", mode='markers')
    fig.show()
    
    fig = masInj.iplot(asFigure=True, xTitle="Injured",
                       yTitle="tonns, mln", title="Cargo")
    fig.show()
            

            
                                                         
                

    #Вывод Количества ДТП
    #Преобразования сделаны, код не актуален
    '''
    masTmp = pd.read_sql_query('select LSD.Region as "Region", LSD.year as "year", ' +
                               'LSD.S/LSD.L as "death", LSD.D/LSD.L as "injured" ' +
                               'from(select Region, "year", sum(NumDTP) as L , '+
                               'sum(DeathDTP) as S, sum(InjDTP) as D from tableDTP ' +
                               'group by Region, "year") as LSD', cnx)
    
    regions = pd.read_sql_query('SELECT Region'+
                                ' FROM tableDTP group by Region', cnx)
    
    masDeath = pd.DataFrame(index = regions['Region'].to_list(),
                            columns = ['2014', '2015', '2016',
                                       '2017', '2018', '2019',
                                       '2020'])
    masInj = pd.DataFrame(index = regions['Region'].to_list(),
                          columns = ['2014', '2015', '2016',
                                     '2017', '2018', '2019',
                                     '2020'])
    
    for i, row in masTmp.iterrows():
        masDeath.loc[row['Region']][str(row['year'])] = row['death']
        masInj.loc[row['Region']][str(row['year'])] = row['injured']

    tempDB.createTable('CREATE TABLE IF NOT EXISTS tableDTPdeath (Region TEXT,'+
                       'y2014 integer,'+
                       'y2015 integer, y2016 integer,'+
                       'y2017 integer, y2018 integer,'+
                       'y2019 integer, y2020 integer,'+
                       'PRIMARY KEY(Region));');
    tempDB.createTable('CREATE TABLE IF NOT EXISTS tableDTPinjured (Region TEXT,'+
                       'y2014 integer,'+
                       'y2015 integer, y2016 integer,'+
                       'y2017 integer, y2018 integer,'+
                       'y2019 integer, y2020 integer,'+
                       'PRIMARY KEY(Region));');

    for i, row in masDeath.iterrows():
        tempDB.insertValue('INSERT INTO tableDTPdeath (Region,'+
                           'y2014, y2015, y2016, y2017, y2018, y2019, y2020 ) VALUES("' +
                           str(row.name) + '","' +                           
                           str(row['2014']) + '","' +
                           str(row['2015']) + '","' + str(row['2016']) + '","' +
                           str(row['2017']) + '","' + str(row['2018']) + '","' +
                           str(row['2019']) + '","' + str(row['2020']) + '");')
    print(masDeath)
    for i, row in masInj.iterrows():
        tempDB.insertValue('INSERT INTO tableDTPinjured (Region,'+
                           'y2014, y2015, y2016, y2017, y2018, y2019, y2020 ) VALUES("' +
                           str(row.name) + '","' +                           
                           str(row['2014']) + '","' +
                           str(row['2015']) + '","' + str(row['2016']) + '","' +
                           str(row['2017']) + '","' + str(row['2018']) + '","' +
                           str(row['2019']) + '","' + str(row['2020']) + '");')
    print(masInj)
    '''

    
    
         
        
    
    
    
    #Вывод Всех графиков
    '''
    fig = masAge.iplot(asFigure=True, xTitle="Year",
                       yTitle="tonns, mln", title="Cargo")
    fig.show()
    
    fig = masMeds.iplot(asFigure=True, xTitle="Year",
                       yTitle="tonns, mln", title="Cargo")
    fig.show()
    
    fig = masCargo.iplot(asFigure=True, xTitle="Year",
                       yTitle="tonns, mln", title="Cargo")
    fig.show()
    
    fig = masFed.iplot(asFigure=True, xTitle="Year",
                       yTitle="tonns, mln", title="Cargo")
    fig.show()
    
    fig = masReg.iplot(asFigure=True, xTitle="Year",
                       yTitle="tonns, mln", title="Cargo")
    fig.show()
    '''
    
    
        
    
        
        
   
    
    tempDB.closeDB()
    
if __name__=="__main__":
    main()













    
