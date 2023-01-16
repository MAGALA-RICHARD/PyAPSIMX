path = r'C:\Users\rmagala\Box\p\APSIMwatershedSImulations\weatherdata'
import os
os.chdir(path)
import pandas as pd
import numpy as np
import arcpy

df = pd.read_csv('Simulaytedresultsx.csv')
fd  = df.convert_dtypes()
gb = r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF_DATA\sievers_case_study\soilMudCreek_20221110\RMagala_20221110\soils_MudCreek_Sievers.gdb'
ap = df.to_records(index= False)
name = os.path.join(gb, 'Mudcreeksimulatedresults3')
sr = arcpy.SpatialReference(4326)


# change some aunassigned data type
dt = ap.dtype.descr
dt[8] = ('Soiltype', '<U25')
dt[4] = ('CompName', '<U25')
featuretobe = ap.astype(dt)
out = arcpy.da.NumPyArrayToFeatureClass(featuretobe, name, ["longitude","Latitude"], sr)
arcpy.GetMessage(0)
from arcgis.features import GeoAccessor, GeoSeriesAccessor
sdf = pd.DataFrame.spatial.from_xy(df=df,
x_column="longitude",
y_column="Latitude",sr=4326)

name = name +"method2"
# Save your feature class in a gdb (or in .shp,...)
sdf.spatial.to_featureclass(location=name, overwrite=True)
