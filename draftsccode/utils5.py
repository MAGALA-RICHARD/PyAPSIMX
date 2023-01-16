# config
pym = r'C:\Users\rmagala\Box\ACPF_MyProject\APSIM scripting data\pyapsimx\python scripts'
import sys
import traceback
sys.path.append(pym)
import multiprocessing
import threading
import pyweather11
import os
import APSIMrun
import weather2
from pyweather11 import daymet_bylocation
from pysoil3 import Replace_Soilprofile2
import pysoil3
from APSIMrun import runAPSIM2
from weather2 import Weather2
import numpy as np
import arcpy
import time
import pandas as pd
from pyproj import transform, CRS, Transformer
#path for the apsimx file
path  = 'C:\\Users\\rmagala\\Box\\ACPF_MyProject\\ACPF_DATA\\APSIM_DATA'
# Now set working directory
wd = r'C:\Users\rmagala\Box\PEWI__DATA\APSIM_OUTPUT'

seriesnames=['Clarion','Canisteo', 'Coland',  'Muscatine', 'Okoboji', 'Nicollet',  "Colo", 'Gara','Tama', 'Webster', 'Buckney',
'Ackmore', 'Nodaway', 'Colo']
series = {'Clarion': (-93.880227,42.049705), 'Okoboji': (-93.900227,42.049705), 'Webster':(-93.900227,42.049705),
          'Canisteo': (-93.900227,42.04970), 'Nicollet': (-93.835782,42.078536), 'Buckney':(-93.890732, 42.041544),
           'Coland': (-93.890732,42.041544), "Colo": (-93.060069, 41.67018), 'Ackmore':(-93.060069, 41.67018),
            'Downs': (-93.141639,   41.605440),'Gara': (-93.030585, 41.652741), 'Nodaway': (-93.074647,  41.606982),
            'Muscatine': (-93.044683, 41.652082), 'Tama':(-93.042241, 41.652082), 'Colo':(-92.955551,  41.606709)}
import multiprocessing
basefile = 'C:\\Users\\rmagala\\Box\\ACPF_MyProject\\base.apsimx'
startyear = 2011
endyear = 2015


#Spool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 2, maxtasksperchild=3)
def worker(basefile, lonlat, startyear, endyear, array, index, report = 'MaizeR'):
      try:
        df = {}
        path2apsimx = Replace_Soilprofile2(basefile, 'domtcp', lonlat, crop = None)
        # download weather data from daymet returns path2file
        weatherpath = daymet_bylocation(lonlat, startyear, endyear)
        wp = Weather2(path2apsimx, weatherpath)
        editedapsimx  = wp.ReplaceWeatherData()
        dat = runAPSIM2(editedapsimx, select_report  = report)
        df['Yield']= dat.Yield.mean()
        df['N20'] = dat.TopN2O.mean()
        df['AGB'] = dat.AGB.mean()
        df['OBJECTID'] = array[index][0]
        df['Shape'] = array[index][1]
        df["CompName"] = array[index][11]
        df["gridcode"] = array[index][3]
        df["MUKEY"] = array[index][4]
        print(df)
        db = editedapsimx.split("apsimx")[0] + "db"
        del dat
        os.remove(db)
        os.remove(editedapsimx)
        os.remove(weatherpath)
        return df
      except:
        pass
#now call list comprehension
# dd = []
# lonlat = array["Shape"]
# m= 0
# start = time.perf_counter()
# for i in range(80):
#   dd.append(worker(basefile, lonlat[i], startyear, endyear, array, i, report = 'MaizeR') )
#   m +=i
#   print(m)
# end = time.perf_counter()
# print(f"time taken is: {end -start}")
#
def Merge(dict1, dict2):
    return(dict2.update(dict1))
##dd = []
##for i in seriesnames:
##  dd.append(worker(i))
arcpy.env.scratchWorkspace = "in_memory"
arcpy.env.workspace = r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF_DATA\sievers_case_study\soilMudCreek_20221110\RMagala_20221110\soils_MudCreek_Sievers.gdb'
#arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
arcpy.env.overwriteOutput = True
class SoilRasterManagement:
  def __init__(self, in_raster, horizontable, rasterfield = "MUKEY"):
    self.raster = in_raster
    self.field = rasterfield
    self.Horizontable = horizontable
  def Organise_soils(self, cellsize =30):
    try:
      start = time.perf_counter()
      shapename = r'in_memory/shapelayer'
      if arcpy.Exists(shapename):
        arcpy.management.Delete(shapename)
      shapename = r'in_memory/shapelayer'
      arcpy.AddMessage('converting raster to polygon')
      self.rasterpolygon =  arcpy.conversion.RasterToPolygon(self.raster , shapename, simplify ="NO_SIMPLIFY", raster_field =self.field)
      
      name = r'in_memory/feature_to_point'
      if arcpy.Exists(name):
        arcpy.management.Delete(name)
      # redefine it
      name = r'in_memory/feature_to_point'
      print('converting polygon to points')
      arcpy.AddMessage('converting Polygon to points')
      # confirm to points
      self.rasterpoints = arcpy.management.FeatureToPoint(self.rasterpolygon , name, 'INSIDE')
      inFeatures = self.rasterpoints
      joinTable = self.Horizontable
      joinField = "MUKEY"
      expression = "CompKind = 'Series'"
      outFeature = "spatialsoilhorizon"
      print("joining tables")
      self.joinedtable = arcpy.management.AddJoin(inFeatures, joinField, joinTable, 
                                                  joinField)
      print("Selecting only soil series") 
      selectionname = "Series"
      # select only points with valid componet names
      self.soilbyseries = arcpy.Select_analysis(self.joinedtable, where_clause = 'CompKind = \'' + selectionname + '\'')
      
      # covert to numpyaray. it is 100 times faster
      sr = arcpy.SpatialReference(4326)
      print("Converting to structured numpy array")
      arcpy.AddMessage("Converting to structured numpy array")
      # searhc the fields
      listfield = []
      lf = arcpy.ListFields(self.soilbyseries)
      for i in lf:
        listfield.append(i.name)
      # cordnates are in position number two
      self.feature_array  = arcpy.da.FeatureClassToNumPyArray(self.soilbyseries,  listfield[:13], spatial_reference = sr)
      return self
    except:
           tb = sys.exc_info()[2]
           tbinfo = traceback.format_tb(tb)[0]
      
           # Concatenate information together concerning the error into a message string
           pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
           print(pymsg + "\n")
      
           if arcpy.GetMessages(2) not in pymsg:
              msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"
              arcpy.AddError(msgs)
              arcpy.AddMessage(msgs)
      
    finally:
            end = time.perf_counter()
            arcpy.AddMessage(f'conversion took: {end-start} seconds')
            print(f'Conversion took: {end-start} seconds')
# ap = SoilRasterManagement('gSSURGO', 'SurfHrz070801030303') # if it from geodatabase
# pp =ap.Organise_soils()

# get results from a pool asyn class
class Result():
    def __init__(self):
        self.val = None

    def update_result(self, val):
        self.val = val

result = Result()

def f(x):
    return x*x

#pool.apply_async(f, (10,), callback=result.update_result)
#expand grid fucntion
from itertools import product
# creating grid expansion
def expand_grid(dictionary):
   """Create a dataframe from every combination of given values."""
   return pd.DataFrame([row for row in product(*dictionary.values())], 
                       columns=dictionary.keys())
def makedf(listitem):
  import pandas
  free = []
  for i in listitem:
    if i !=None:
     free.append(i)
  df = pandas.DataFrame.from_dict(free)
  return df
# this function cleans up met, apsimx and met files after simulations
def removefiles(iterable):
  for i in iterable:
    os.remove(i)
# get the main directory
def CLeaUp(ws):
    pt = ws
    # delete apsimx files
    ran = os.path.join(pt, 'APSIMwatershedSImulations')
    apsimfiles = glob.glob1(ran, "*.apsimx")
    os.chdir(ran)
    removefiles(apsimfiles)
    # delete db files
    dbfiles = glob.glob1(ran, "*.db")
    removefiles(dbfiles)
    #delete excel files
    excelfiles= glob.glob1(ran, "*.csv")
    removefiles(excelfiles)
    # del weather
    wther = os.path.join(ran, 'weatherdata')
    os.chdir(wther)
    metfiles = glob.glob1(wther, "*met")
    removefiles(metfiles)
    print("clean up successful")

# evaluate lists
def notin(targetlist, compare):
  py = []
  for i in compare:
    if i not in targetlist:
      py.append(i)
      #print(i)
  return py
def changedtype(data):
  dt = data.dtype
  dt = dt.descr # this is now a modifiable list, can't modify numpy.dtype
  # change the type of the first col:
  dt[0] = (dt[0][0], 'float64')
  dt = numpy.dtype(dt)
  # data = numpy.array(data, dtype=dt) # option 1
  data = data.astype(dt)
  
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
def Mainrun(): 
       pr = [worker(i, basefile, start, end, array) for i in a]
       listp = []
       for i in pr:
        if i != None:
          listp.append(i)
       return listp
def MainrunforMP(index): # this index is gonna come from the list a
       pr = worker(index, basefile, start, end, array)
       return pr   
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

