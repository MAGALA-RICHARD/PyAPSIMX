import requests
import pandas as pd
import json
baseurl  = "http://mesonet.agron.iastate.edu/geojson/network.php?network="
        # download weather stations only in Iowa
        stateclim  =  baseurl + 'IA' + 'CLIMATE'
        print(stateclim)
        rep = requests.get(stateclim)
        # Check the response object
        print(rep.ok)
        print(rep)
       
        # check the type of data returned
        rep.headers['content-type']
        #load the data to computer memory
        rep_content  = rep.content
        #convert it to a dictionary
        rep_json  = json.loads(rep_content)
        # let's extract only the features and make a data frame
        features = rep_json['features']
        # use pandas to that
        # this data is a list of objects so json normalise works prety well
        df =pd.json_normalize(features)
        df.head()
                    
