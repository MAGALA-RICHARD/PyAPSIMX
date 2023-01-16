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

#Spool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 2, maxtasksperchild=3)
def worker(basefile, lonlat, startyear, endyear, report = 'P_3051_T'):
  try:
    # downloads and replaces soilprofile in apsimx file
        path2apsimx = Replace_Soilprofile2(basefile, 'domtcp', lonlat, crop = None)
        # download weather data from daymet returns path2file
        weatherpath = daymet_bylocation(lonlat, startyear, endyear)
        wp = Weather2(path2apsimx, weatherpath)
        editedapsimx  = wp.ReplaceWeatherData()
        if not editedapsimx:
          pass
        else:
          pass
        dd = runAPSIM2(editedapsimx, select_report  = report)
        # if successful, remove
        return dd
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


from itertools import product
# creating grid expansion
def expand_grid(dictionary):
   """Create a dataframe from every combination of given values."""
   return pd.DataFrame([row for row in product(*dictionary.values())], 
                       columns=dictionary.keys())

