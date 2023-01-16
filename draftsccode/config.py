import sys
import os
sys.path.append(r'C:\Users\rmagala\Box\ACPF_MyProject\APSIM scripting data\pyapsimx\python scripts')
import pysoil2
import weather2
import APSIMrun
import pandas as pd
from pyweather9 import dowload_iem_bylocation1
from pyweather10 import daymet_bylocation
# Step 1 import the look up table
lp = r'C:\Users\rmagala\Downloads\jis'
path = r'C:\Users\rmagala\Box\ACPF_MyProject\APSIM scripting data\pyapsimx'
lookup = os.path.join(lp, 'look uptable.csv')
simdates= ['01-01-2000', '12-31-2020']
#step 2 download the weather
table = pd.read_csv(lookup)
apx= []
for i in table.centroid.iteritems():
  ap = i[1][1:][:-1].split()
  cor = [ap[0][:-1], ap[1]]
  cods = [float(y) for y in cor]
  apx.append(cods)
for m in apx:
  y = dowload_iem_bylocation1(simdates, apx[16],  path, state ="Iowa", dist = 175000, name ="featurestation")
  print('downloading', i, '\n')
  
  for i in apx:
    mx = daymet_bylocation(i, 2000, 2020)
aps = r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF DATA\APSIM_DATA'
weatherdata = r'C:\Users\rmagala\Box\ACPF_MyProject\APSIM scripting data\pyapsimx\weatherdata'
for item in os.listdir(weatherdata):
  wp = editapsimx(aps, 'Base.apsimx', item, weatherdata)
  wp.ReplaceWeatherData()
frame= []
ap =r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF DATA\APSIM_DATA\weather_refilled' 
for i in os.listdir(ap):
  if i.endswith(".apsimx"):
    df = runAPSIM(ap, i, select_report= 'P_3051_T')
    frame.append(df)
