import arcpy
pym = r'C:\APSIM_Simulations\python_scriptsx'
import sys
sys.path.append(pym)
import traceback
import sys
import time
import copy
import os
from platform import python_version
import winsound
import platform
import utils1
import multiprocessing
import threading
import pyweather11
import os
import APSIMrun
import weather2
from pyweather11 import daymet_bylocation
from pysoil4 import Replace_Soilprofile2
import pysoil4
from APSIMrun import runAPSIM2
from weather2 import Weather2
import numpy as np
import arcpy
import time
import glob
from multiprocessing import Pool, freeze_support
import multiprocessing as mp
import utils1
import shutil
from pyproj import transform, CRS, Transformer
# set up the environment
arcpy.env.scratchWorkspace = 'in_memory'
arcpy.AddMessage('scratch workspace is in: ' + str(arcpy.env.scratchWorkspace))
arcpy.env.overwriteOutput = True
arcpy.env.workspace = r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF_DATA\sievers_case_study\soilMudCreek_20221110\RMagala_20221110\soils_MudCreek_Sievers.gdb'
# get user inputs

pt =  r'C:\Users\rmagala\Box'

apsimxbasefile = os.path.join(pt, "corn.apsimx")
basefile = apsimxbasefile
in_raster = 'gSSURGO'
hosoilhorizontable = 'SurfHrz070801030303'
startyear= 2000
endyear = 2020
cleanup = True
reportname = "MaizeR"
ws  = r'C:\Users\rmagala\Box\p'
# redine the variables
start = int(startyear)
end  = int(endyear)
print('Starting simulation', flush = True)
basefile = apsimxbasefile
reported =reportname
# change workspace
os.chdir(ws)
if not os.path.exists("APSIMwatershedSImulations"):
    os.makedirs("APSIMwatershedSImulations")
os.chdir("APSIMwatershedSImulations")
if not os.path.exists("SimulationResults"):
    os.makedirs("SimulationResults")
soils= utils1.SoilRasterManagement(in_raster, horizontable = hosoilhorizontable)
print('Creating lookup info')
soilinfo = soils.Organise_soils()
# Set up the worker function
ar = soilinfo.feature_array
array1 =soilinfo.feature_array[:100]
array2 = ar[400:]
ar1 = array1[['feature_to_point_gridcode', "Shape", 'OBJECTID']]
ar2 = array2[["Shape", 'feature_to_point_gridcode', "OBJECTID"]]
no1 = len(ar1)
no2  = len(ar2)
a = list(np.arange(no1))

b = np.arange(no2)

print("===============================================")
array = ar1
index= 0
start = int(startyear)
end = int(endyear)
#print(runAPSIM2(editedapsimx))
print("===============================================")
def worker(index, basefile, start, end, array):
      try:
        df = {}
        array = ar1
        report  =reported
        start= int(startyear)
        end = int(endyear)
        basefile = basefile
        path2apsimx = Replace_Soilprofile2(basefile, 'domtcp', array[index][1], filename = array[index][2], gridcode = str(array[index][0]), Objectid = str(array[index][2]), crop = None)
        # download weather data from daymet returns path2file
        weatherpath = daymet_bylocation(ar1[index][1], start, end)
  
        wp = Weather2(path2apsimx, weatherpath)
        arcpy.AddMessage(type(wp))
        editedapsimx  = wp.ReplaceWeatherData()
        arcpy.AddMessage(type(editedapsimx))
        return editedapsimx
      except:
        pass
path = f'{ws}\\APSIMwatershedSImulations\\*.apsimx'
apsimxfiles = (glob.glob(path))

def Mainrun(): 
   pr = [worker(i, basefile, start, end, array) for i in a]
   listp = []
   for i in pr:
    if i != None:
      listp.append(i)
   return listp

# replicate the above
def MainrunforMP(index): # this index is gonna come from the list a
       pr = worker(index, basefile, start, end, array)
       return pr
#collectformaize
def CollectforMaize(apsimx):
    try:
        df = {}
        report = "MaizeR"
        dat = runAPSIM2(apsimx)
        df['longitude'] = dat["MaizeR"].longitude.values[1]
        df["Latitude"] = dat["MaizeR"].latitude[0]
        df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
        df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
        df["CompName"] = dat["MaizeR"].soiltype.values[1].split(":")[0]
        df['meanMaizeYield']= dat["MaizeR"].Yield.mean()
        df['meanMaizeAGB'] =  dat["MaizeR"].AGB.mean()
        df["ChangeINCarbon"]    =dat['Carbon'].changeincarbon[0]
        df['meanN20'] = dat["MaizeR"].TopN2O.mean()
        df["Soiltype"] = dat["MaizeR"].soiltype.values[1]
        df["CO2"] = dat['Annual'].Top_respiration.mean()
        df["meanSOC1"] = dat['Annual'].SOC1.mean()
        df["meanSOC2"] = dat['Annual'].SOC2.mean()
        
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

    # End main
def notin(targetlist, compare):
      py = []
      for i in compare:
        if i not in targetlist:
          py.append(i)
          #print(i)
          return py
if __name__ == "__main__":
    mp.set_executable(os.path.join(sys.exec_prefix, 'python.exe'))
    freeze_support()
    mp.set_start_method('spawn')
    
    # first clean up
    def DownloadMultipleweatherfiles(function, variables):
        """<function, variables> Execute a process on multiple processors.
        INPUTS:
        function(required) Name of the function to be executed.
        variables(required) Variable to be passed to function.
        Description: This function will run the given fuction on to multiprocesser. Total number of jobs is equal to number of variables.        
        """
        with Pool(processes=20) as pool:
            mpweather= pool.map(function, variables, chunksize = 50)
            listmp = []
            for i in mpweather:
                if i != None:
                  listmp.append(i)
            return listmp
        
    print(f"Downloading Weather and Soils Data..............", flush=True)
    s = time.perf_counter()
    #data = Mainrun()
    data  = DownloadMultipleweatherfiles(MainrunforMP, a)
    e = time.perf_counter()
    print(f"Downloading weather data took: {e-s} seconds")
    def run_MultiPros(function, variables):
        """<function, variables> Execute a process on multiple processors.
        INPUTS:
        function(required) Name of the function to be executed.
        variables(required) Variable to be passed to function.
        Description: This function will run the given fuction on to multiprocesser. Total number of jobs is equal to number of variables.        
        """
        with Pool(processes=20) as pool:
            #pp = [pool.(function, (i,)).get() for i in variables] #apply_async takes on multiple arguments in a tupply form
            pp =[]
            for i in variables:
               p=  pool.apply_async(function, (i,)).get()
               pp.append(p)
            px = utils1.makedf(pp)
            print(px)
            # run again
            resultsdir = os.path.join(os.getcwd(), "SimulationResults")
            sim = 'Simulaytedresultsx.csv'
            results = os.path.join(resultsdir, sim)
            if os.path.exists(results):
                os.remove(results)
            results = os.path.join(resultsdir, sim)
            px.to_csv(results)
            return px
            s = time.perf_counter()
    #pain(data, runapsimx)
    print(f"Now running the simulations please wait...........You can get a cup of coffee as you wait", flush=True)
    
##    pain= [runapsimx(x) for x in data]
##    arcpy.AddMessage(pain)
##    pp = utils1.makedf(pain)
##    print(pp)
##    sim = 'Simulaytedresultsx.csv'
##    if os.path.exists(os.path.join(os.getcwd(), sim)):
##      os.remove(os.path.join(os.getcwd(), sim))
##    sim = 'Simulaytedresultsx.csv'
##    pp.to_csv(sim)
##    e = time.perf_counter()
##    print(f"running APSIM took: {e-s} seconds")
    
    # clean up files
    s = time.perf_counter()
    run_MultiPros(CollectforMaize, data)
    e = time.perf_counter()
    print(f'multiprocesing took: {e-s}seconds')
    if cleanup:
        import utils3
        utils3.CLeaUp(ws)
        print("Done*****")
    
