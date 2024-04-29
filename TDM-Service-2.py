import asyncio
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
import json
from geopy.distance import geodesic 
#from turfpy.measurement import boolean_point_in_polygon
from geojson import Point, Polygon, Feature #possibly replace with geopandas
from shapely.geometry import Polygon, Point
import csv
import ast


def read_polygons_from_csv(csv_file):
    polygons = []
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            try:
                polygon_name = row[0]
                #print("this is it" + row[1])
                points = [(float(row[i]), float(row[i + 1])) for i in range(1, len(row), 2)]
                polygon = Polygon(points)
                polygons.append({'name': polygon_name, 'polygon': polygon})
            except Exception as e:
                e
    return polygons

def point_inside_polygons(point, polygons):
    point = Point(point)
    for poly_data in polygons:
        if point.within(poly_data['polygon']):
            return poly_data['name']
    return None



async def firstLoop():
    # Establish an initial connection to a NATS server given a port
    # Sub to the desired subject
    # nc = await nats.connect("nats://127.0.0.1:4222")
    # sub = await nc.subscribe("machine.*.pos")
    nc = await nats.connect(

        "nats://pipeline.ecn.purdue.edu", user_credentials="C:\\Users\\kvs62\\OneDrive\\Desktop\\TDM(2).creds"
    )



    sub = await nc.subscribe("machine.*.pos")
    # Every time sub receives a message, enter this for loop
    async for msg in sub.messages:
        try: 
            print(msg.subject)
            id = msg.subject.split('.')[1] 
            previous_entered = False 
            in_csv = False
            exit = False 
            polygon_name = "test_polygon"
            time_entered = ""   
            current_time = ""
            context = ""
            # Print the subject and message
            print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
            original_string = msg.data.decode()
            modified_string = original_string.replace('/', '"')
            # Load the string received by messages as a JSON file
            # Automatically creates a python dictionary (json.loads)
            json_obj = json.loads(modified_string)
            # Print any values in the JSON file according to keys
            # ex.(["object"] takes everything after "object": in JSON data
            # until next key (note the brackets {}))
            #print(json_obj["time"])
            #print(json_obj["latitudeDeg"])


            # set the longitude and latitude points for the JSON
            longitude = json_obj["longitudeDeg"]
            latitude = json_obj["latitudeDeg"]
            current_time = json_obj["time"]
            context = json_obj["context"]
            # set the longitude and latitude points for desired comparison / polygon
            ABE_longitude = 40.42167305035119
            ABE_latitude = -86.91657055932582
            coords_1 = (longitude, latitude)
            coords_2 = (ABE_longitude, ABE_latitude)

            #print to see distance/if inside polygon
            #JSON format id, previous_status, polygon, previous_time
            #create table of machienes, to keep track of when something has entered and when something has exited
            file_name = "C:\\Users\\kvs62\\OneDrive\\Desktop\\Nats2.csv"
            polygon_file = "C:\\Users\\kvs62\\OneDrive\\Desktop\\Polygon csv - Sheet1.csv"
            polygons_data = read_polygons_from_csv(polygon_file)
            result = point_inside_polygons(coords_1, polygons_data)
            #print(result)
            with open(file_name, mode='r') as file:
                csvreader = csv.reader(file)
                next(csvreader)
                for row in csvreader:
                    if row:
                        if (row[0] == id):
                            #print("found")
                            if row[1] == "True":
                                previous_entered = True
                            if row[2] == "False":
                                previous_entered = False
                            #print(row[1])
                            polygon_name = row[2]
                            time_entered = row[3]
                            in_csv = True
                            break
            #print(previous_entered)
            if (in_csv == False):
                with open(file_name, 'a') as file:
                    writer = csv.writer(file)
                    if (result != None):
                        writer.writerow([str(id),True,result,str(current_time)])
                    else:
                        writer.writerow([str(id),False,"NO_POLYGON",str(current_time)])
                    print("wrote to file")
            #print(previous_entered)
            #print("result: " + str(result))
            if result is not None and previous_entered == False:
                data = {
                    "machine": id,
                    "time": current_time,
                    "concert": context
                }
                data2 = {
                    "time": current_time,
                    "polygon": result,
                    "context": context #TODO add context
                }
                message = json.dumps(data2)
                message2 = json.dumps(data)
                #Polygon Publish: time, machiene, event type (enter/exit), etc etc poly.id.event
                #Machine Publish: time, polygon, event type (enter/exit), etc etc machine.id.evnet
                await nc.publish(f"machine.{id}.events.enter", message.encode("utf-8"))
                await nc.publish(f"polygon.{result}.events.enter", message2.encode("utf-8"))
                print(message)
                print(message2)
                # Modify the specified line
                with open(file_name, mode='r') as file:
                    csvreader = csv.reader(file)
                    rows = [row for row in csvreader]  # Read all rows into a list

                for row in rows:
                    if row:
                        if row[0] == id:
                            row[1] = True
                            row[2] = result
                            row[3] = json_obj["time"]
                            break
                # Write the modified content back to the file
                with open(file_name, mode='w', newline='') as file:
                    csvwriter = csv.writer(file)
                    csvwriter.writerows(rows)
            elif result is None and previous_entered == True:
                with open(file_name, mode='r') as file:
                    csvreader = csv.reader(file)
                    rows = [row for row in csvreader]  # Read all rows into a list
                for row in rows:
                    if row:
                        if row[0] == id:
                            result = row[2]
                print(result)

                print("exited")
                data = {
                    "machine": id,
                    "time": current_time,
                    #TODO add context
                    "concert": context
                }
                data2 = {
                    "time": current_time,
                    "polygon": result,
                    #TODO add context
                    "concert": context
                }
                message = json.dumps(data2)
                message2 = json.dumps(data)
                await nc.publish(f"machine.{id}.events.exit", message.encode("utf-8"))
                await nc.publish(f"polygon.{str(polygon_name)}.events.exit", message2.encode("utf-8"))
                print(message)
                result = None
                # Modify the specified line
                with open(file_name, mode='r') as file:
                    csvreader = csv.reader(file)
                    rows = [row for row in csvreader]  # Read all rows into a list

                for row in rows:
                    if row:
                        if row[0] == id:
                            row[1] = False
                            row[3] = json_obj["time"]
                            break
                # Write the modified content back to the file
                with open(file_name, mode='w', newline='') as file:
                    csvwriter = csv.writer(file)
                    csvwriter.writerows(rows)
            print("Current distance from ABE: " + str(geodesic(coords_1, coords_2).km) + "km" ) 
            # If "exit" is receieved, unsubscribe and terminate the thread.
            if (msg.data.decode() == 'exit'):
                await sub.unsubscribe()
                exit = True
        except Exception as e:
            print("Threw on first " + str(e))
            raise(e)
    await nc.drain



loop = asyncio.new_event_loop() 
asyncio.set_event_loop(loop)
asyncio.ensure_future(firstLoop()) 
loop.run_forever()
