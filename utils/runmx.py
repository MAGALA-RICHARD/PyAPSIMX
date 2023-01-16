pym = r'C:\APSIM_Simulations\python_scriptsx'
#pym = r'C:\Users\rmagala\Box\ACPF_MyProject\APSIM scripting data\pyapsimx\python scripts'
import sys
sys.path.append(pym)
import glob
import os
import utils1
import arcpy
from multiprocessing import Pool, freeze_support
import multiprocessing as mp
from APSIMrun import runAPSIM2
import numpy as np
import time
from pysoil4 import Replace_Soilprofile2
from pyweather11 import daymet_bylocation
from weather2 import Weather2
import copy
import traceback
import json
import math
import winsound
import platform
import sys
import shutil
# collect user info from the toolbox by reading the dropped json object
try:
    
        def collectsoilinfo(in_raster, horizontable):
                soils= utils1.SoilRasterManagement(in_raster, horizontable)
                soilinfo = soils.Organise_soils()
                ar = soilinfo.feature_array
                return ar
        def worker(index, basefile, start, end, array):
          try:
                basefile = basefile
                path2apsimx = Replace_Soilprofile2(basefile, 'domtcp', array[index][1], filename = array[index][2], gridcode = str(array[index][0]), Objectid = str(array[index][2]), crop = None)
                # download weather data from daymet returns path2file
                weatherpath = daymet_bylocation(array[index][1], start, end)
                wp = Weather2(path2apsimx, weatherpath)
                editedapsimx  = wp.ReplaceWeatherData()
                return editedapsimx
          except:
                #pass # for now
                tb = sys.exc_info()[2]
                tbinfo = traceback.format_tb(tb)[0]

                #Concatenate information together concerning the error into a message string
                pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
                print(pymsg + "\n")

        def DownloadMultipleweatherfiles(function, variables):
                row_count  = len(variables)
                wvar = []
                for i in variables:
                    var = wvar.append(function(i))
                    print('downloading soils and weather for #', str(i))
                #mpweather= list(map(function, variables))
                listmp = []
                for i in wvar:
                        if i != None:
                          listmp.append(i)
                return listmp

        def runapsimx(apsimx): 
          try:
                df = {}
                report = "MaizeR"
                dat = runAPSIM2(apsimx)
                df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
                df["Shape"] = dat["MaizeR"].longitude.values[1], dat["MaizeR"].latitude[0]
                df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
                df["CompName"] = dat["MaizeR"].soiltype.values[1].split(":")[0]
                df['meanMaizeYield']= dat["MaizeR"].Yield.mean()
                df['meanMaizeAGB'] =  dat["MaizeR"].AGB.mean()
                df["Soiltype"] = dat["MaizeR"].soiltype.values[1]
                df['longitude'] = dat["MaizeR"].longitude.values[1]
                df["Latitude"] = dat["MaizeR"].latitude[0]
                df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
                df["ChangeINCarbon"]    =dat['Carbon'].changeincarbon[0]
                df["RyeBiomass"] = dat["WheatR"].AGB.mean()
                df["CO2"] = dat['Annual'].Top_respiration.mean()
                df["meanSOC1"] = dat['Annual'].SOC1.mean()
                df["meanSOC2"] = dat['Annual'].SOC2.mean()
                df['meanN20'] = dat["MaizeR"].TopN2O.mean()
                return df
          except:
            pass
        def CollectforMaize(apsimx):
          try:
                df = {}
                report = "MaizeR"
                dat = runAPSIM2(apsimx)
                df['meanMaizeYield']= dat["MaizeR"].Yield.mean()
                df['meanMaizeAGB'] =  dat["MaizeR"].AGB.mean()
                df['longitude'] = dat["MaizeR"].longitude.values[1]
                df["Latitude"] = dat["MaizeR"].latitude[0]
                df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
                df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
                df["CompName"] = dat["MaizeR"].soiltype.values[1].split(":")[0]
                df["Soiltype"] = dat["MaizeR"].soiltype.values[1]
                df["CO2"] = dat['Annual'].Top_respiration.mean()
                df["meanSOC1"] = dat['Annual'].SOC1.mean()
                df["meanSOC2"] = dat['Annual'].SOC2.mean()
                df['meanN20'] = dat["MaizeR"].TopN2O.mean()
                df["ChangeINCarbon"]    =dat['Carbon'].changeincarbon[0]

                return df
          except:
           pass
        def runapsimx(apsimx): 
                try:
                  df = {}
                  report = "MaizeR"
                  dat = runAPSIM2(apsimx)
                  df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
                  df["Shape"] = dat["MaizeR"].longitude.values[1], dat["MaizeR"].latitude[0]
                  df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
                  df["CompName"] = dat["MaizeR"].soiltype.values[1].split(":")[0]
                  df['meanMaizeYield']= dat["MaizeR"].Yield.mean()
                  df['meanN20'] = dat["MaizeR"].TopN2O.mean()
                  df['meanMaizeAGB'] =  dat["MaizeR"].AGB.mean()
                  df["Soiltype"] = dat["MaizeR"].soiltype.values[1]
                  df['longitude'] = dat["MaizeR"].longitude.values[1]
                  df["Latitude"] = dat["MaizeR"].latitude[0]
                  df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
                  df["ChangeINCarbon"]    =dat['Carbon'].changeincarbon[0]
                  df["RyeBiomass"] = dat["WheatR"].AGB.mean()
                  df["CO2"] = dat['Annual'].Top_respiration.mean()
                  df["meanSOC1"] = dat['Annual'].SOC1.mean()
                  df["meanSOC2"] = dat['Annual'].SOC2.mean()
                  return df
                except:
                  pass
except:
        
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]

    # Concatenate information together concerning the error into a message string
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
    print(pymsg + "\n")
    

    if arcpy.GetMessages(2) not in pymsg:
        msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"
        arcpy.AddError(msgs)
        print(msgs)     
