import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient

#google api key: AIzaSyBHqrFWhMMXsMbw8u44wwMlymllopjKTVs
#ie : https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key=YOUR_API_KEY
class propertyaccess(dml.Algorithm):
     contributor = 'lwj'
     reads = []
     writes = ['lwj.propertydb']

     @staticmethod
     def findloc(ad):
          context = ssl._create_unverified_context()
          newad = ""
          for i in range(len(ad)):
               if(ad[i] != " "):
                    newad += ad[i]
               else:
                    if(newad[-1] != "+"):
                         newad += "+"
          if(newad[-1] == "+"):
               newad = newad[:-1]
          suf = "&key=AIzaSyBHqrFWhMMXsMbw8u44wwMlymllopjKTVs"
          url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + newad + suf
          response = urllib.request.urlopen(url, context = context).read().decode("utf-8")
          loc = json.loads(response)
          x = loc['results'][0]["geometry"]['location']['lat']
          y = loc['results'][0]["geometry"]['location']['lng']
          #print(x,y)
          return (x,y)
     
     @staticmethod
     def projection(property_data):
          propertyinfo = []
          count = 0
          count2 = 0
          addrbook = []
          c = 0
          print("Start analyzing propery_Data")
          for i in range(len(property_data)):
               if(i > c * 0.1 * len(property_data)):
                    print(str(c * 10) + "% ...",)
                    c += 1
               try:
                    ad = property_data[i][21] + " " + property_data[i][22]
                    if(ad not in addrbook):
                              addrbook.append(ad)
                    else:
                         raise
                    #print(ad)
                    x,y = propertyaccess.findloc(ad)
                    if(x is not None and y is not None):
                         x = str(abs(round(x * 1000000)))
                         y = str(abs(round(y * 1000000)))
                         #print(x, y)
                         item = {}
                         item['addr'] = ad
                         item['value'] = property_data[i][26]
                         item['area'] = property_data[i][32]
                         item['x'] = x
                         item['y'] = y
                         #print(item['value'])
                         #print(item['area'])
                         #item['avgvalue'] = str(int(int(item['value'])/(int(item['area'])*1.0)))
                         propertyinfo.append(item)
                         count2 += 1
               except:
                    count += 1
                    continue
         # print(count, "errors")
          #print(count2, "success")
 #         print(propertyinfo[0])
          print("Added " + str(len(propertyinfo)) + " new property")
          return propertyinfo

               

     @staticmethod
     def execute(trial = True):
          startTime = datetime.datetime.now()
          context = ssl._create_unverified_context()
          #have to use download file as the api for this database only provide the first 1000 data,
          #so i have to download it and open it here
          try:
               with open("./lwj/data/property.json","r") as f:
                    response = f.readlines()
          except:
               with open("./data/property.json","r") as f:
                    response = f.readlines()
          res = ""
          for i in response:
               res += i
          #print(type(response))
          property_data = json.loads(res)
          property_data = property_data["data"]
          if trial:
               d = property_data
               property_data = []
               for i in range(100):
                    property_data.append(d[i])
          property_data = propertyaccess.projection(property_data)
          #print(property_data)
          client = dml.pymongo.MongoClient()
          project = client.project
          project.authenticate("lwj", "lwj")
          project.propertydb.drop()
          propertydb = project.propertydb
          propertydb.insert_many(property_data)
          project.logout()
          endTime = datetime.datetime.now()
          return {"star":startTime, "end":endTime}

     @staticmethod
     def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
          doc = prov.model.ProvDocument()    

          doc.add_namespace('alg', 'lwj#union')
          doc.add_namespace('dat', 'lwj#propertydb')
          doc.add_namespace('ont', 'lwj/ontology#')
          doc.add_namespace('log', 'lwj/log/')
          doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
          e = doc.entity('dat:lwj#propertydb', {
              prov.model.PROV_TYPE: "ont:File",
              prov.model.PROV_LABEL:'Propertydb'
          })

          r = doc.entity('bdp:5b5-xrwi', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:property_data', 'ont:Extension':'json'})

          a = doc.agent('alg:lwj#propertyaccess', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extentsion':'py'})

          get_propertyaccess = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

          doc.usage(get_propertyaccess, r, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=mail_address,mail_cs,av_total,living_area'
                  }
                  )

     
          doc.wasAssociatedWith(get_propertyaccess, a)
          doc.wasAttributedTo(e, a)
          doc.wasGeneratedBy(e, get_propertyaccess, endTime)
          doc.wasDerivedFrom(e, r, get_propertyaccess,get_propertyaccess,get_propertyaccess)


          return doc
                            
#propertyaccess.findloc("1 WESTINGHOUSE PZ #C-339 HYDE PARK MA")         
propertyaccess.execute()
#doc = propertyaccess.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))


