#Note: property.json has to be downloaded before running, it is too big to use urlib request

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient
from math import radians, cos, sin, asin, sqrt

class geo(dml.Algorithm):
     contributor = 'lwj'
     reads = ["lwj.access", "lwj.propertydb"]
     writes = ["lwj.property_accessibility"]
     @staticmethod
     def getDis(lon1, lat1, lon2, lat2):
          lon1 = int(lon1)/(1000000.0)
          lon2 = int(lon2)/(1000000.0)
          lat1 = 0 - int(lat1)/(1000000.0)
          lat2 = 0- int(lat2)/(1000000.0)
          lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
          dlon = lon2 - lon1
          dlat = lat2 - lat1
          a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
          c = 2 * asin(sqrt(a))
          km = 6367 * c
          return km
          

     @staticmethod
     def mapD(pes, aes):
          #pes for property
          #aes for access
          #print(pes[0])
          #print(aes[0])
          print("analyzing the property's accessibility")
          print(len(aes))
          for p in pes:
 #              print(p)
               p['access'] = {}
               p['access']['hospital'] = []
               p['access']['garden'] = []
               p['access']['college'] = []
               p['access']['market'] = []
               for a in aes:
                   km = geo.getDis(p['x'], p['y'],  a['x'], a['y'])
                   #print(km)
                   if km < 4:
                        p['access'][a['type']].append(a)
 #         print(pes)
                   
          return pes
          
     @staticmethod
     def execute(trial = True):
          startTime = datetime.datetime.now()
          client = dml.pymongo.MongoClient()
          project = client.project
          project.authenticate("lwj", "lwj")
          access = project.access
          access_find = access.find()
          access_data = []
          for i in access_find:
               access_data.append(i)
 #         print(access_data[0])
          propertydb = project.propertydb
          property_find = project.propertydb.find()

          property_data = []
          for i in property_find:
               property_data.append(i)

 #         print(property_data[0])
 #         geo.getDis(property_data[0]['addr'], access_data[0]['addr'])
          geo.mapD(property_data, access_data)
          project.property_accessibility.drop()
          property_accessibility = project.property_accessibility
          property_accessibility.insert_many(property_data)
          project.logout()
          endTime = datetime.datetime.now()
          return {"start":startTime, "end":endTime}
     
     @staticmethod
     def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
          return doc
                            
          
geo.execute()
#doc = union.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
