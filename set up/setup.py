import bottle
import threading
from queue import Queue


app = bottle.Bottle()

baseurl = 'https://power.larc.nasa.gov/'
lonlat = [-91.72704709, 41.93103233]
lon = lonlat[0]
lat = lonlat[1]
start  = 2000
end = 2002
string2 = 'daily/point?parameters=T2M&community=ag&longitude={0}&latitude={1}'.format(lon, lat)
string3 = '&start={0}0101&end={1}1231&format=ascii'.format(start, end)
query = baseurl + string2 + string3
from requests import get
ap = get(query)
ap
baseurl = 'https://power.larc.nasa.gov/api/temporal/
string2 = 'daily/point?start={0}0101&end={1}1231'.format(start, end)
string3 = '&latitude={0}&longitude={1}&community=ag'.format(lat, lon)
string4 = '&parameters=RH2M&format=csv&user=richard&header=true&time-standard=lst'
querry = baseurl + string2 + string3 + string4
rm = get(querry)
rm

