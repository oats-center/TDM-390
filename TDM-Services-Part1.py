import asyncio
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
import json
from geopy.distance import geodesic 
#from turfpy.measurement import boolean_point_in_polygon
from geojson import Point, Polygon, Feature
import csv
from csv import DictWriter, DictReader
import nkeys
import datetime
import traceback
from datetime import datetime, timedelta

async def firstLoop():
    # Establish an initial connection to a NATS server given a port
    # Sub to the desired subject
    #nc = await nats.connect("nats://127.0.0.1:4222")
    #sub = await nc.subscribe("JSON")

    nc = await nats.connect("nats://pipeline.ecn.purdue.edu", user_credentials="/Users/aaravpai/Desktop/TDM.creds")
    sub = await nc.subscribe('hello')
    #sub = await nc.subscribe('>')
    exit = False

    #await nc.publish('{"deduplicationId":"44557e00-533f-4806-97e3-b49e1ac1ef61","time":"2024-02-05T21:16:41.969+00:00","deviceInfo":{"tenantId":"52f14cd4-c6f1-4fbd-8f87-4025e1d49242","tenantName":"ChirpStack","applicationId":"11a904ca-ce8f-4145-b931-ddadb0418093","applicationName":"TDM","deviceProfileId":"229ad518-2c86-4233-81fa-e811469b70ce","deviceProfileName":"Digital Matter Oyster","deviceName":"oyster-f300","devEui":"70b3d5705000f300","deviceClassEnabled":"CLASS_A","tags":{}},"devAddr":"0167190b","adr":false,"dr":0,"fCnt":3,"fPort":1,"confirmed":false,"data":"AAAAAAAAAAADAM8=","object":{"longitudeDeg":8.0,"batV":5.175,"headingDeg":0.0,"speedKmph":0.0,"latitudeDeg":5.0,"type":"position","fixFailed":true,"inTrip":true,"manDown":null},"rxInfo":[{"gatewayId":"ac1f09fffe0588e7","uplinkId":16677,"time":"2024-02-05T21:16:40.969533+00:00","timeSinceGpsEpoch":"1391203019.969s","rssi":-31,"snr":13.0,"channel":3,"location":{"latitude":40.42138,"longitude":-86.91566,"altitude":184.0},"context":"Oaezlw==","metadata":{"region_common_name":"US915","region_config_id":"us915_1_DR_1_3"},"crcStatus":"CRC_OK"}],"txInfo":{"frequency":904500000,"modulation":{"lora":{"bandwidth":125000,"spreadingFactor":10,"codeRate":"CR_4_5"}}}}', b'First')

    while(exit == False):
        try:
            await asyncio.sleep(0.01)
            # Every time sub receives a message, enter this for loop
            async for msg in sub.messages:
                # Print the subject and message
                print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
                
                # Load the string received by messages as a JSON file
                # Automatically creates a python dictionary (json.loads)
                json_obj = json.loads(msg.data.decode())

                dev_ID = json_obj["deviceInfo"]["devEui"]
                print(type(dev_ID))
                dev_name = ''

                with open('/Users/aaravpai/Desktop/Nats_Names.csv', mode='r') as file:
                    csvreader = csv.reader(file)

                    next(csvreader)
                    for row in csvreader:
                        if (row[1] == dev_ID):
                            dev_name = row[0]
                            print("got name")
                            break

                    if (dev_name == ''):
                        dev_name = "temp_name_to_insert"
                        print("oops")
                
                if (dev_name == "temp_name_to_insert"):
                    with open('/Users/aaravpai/Desktop/Nats_Names.csv', 'a') as file:
                        writer = csv.writer(file)
                        writer.writerow([dev_name, dev_ID])

                # Print any values in the JSON file according to keys
                # ex.(["object"] takes everything after "object": in JSON data
                # until next key (note the brackets {}))
                #print(json_obj["object"]["longitudeDeg"])
                #print(json_obj["object"]["latitudeDeg"])

                time = json_obj["time"]
                print(type(time))
                longitude = json_obj["object"]["longitudeDeg"]
                #longitude = json_obj["longitude"]
                print(type(longitude))
                latitude = json_obj["object"]["latitudeDeg"]
                #latitude = json_obj["latitude"]
                print(type(latitude))
                speed = json_obj["object"]["speedKmph"]
                #speed = json_obj["speed"]
                print(type(speed))
                jtype = json_obj["object"]["type"]
                #jtype = json_obj["type"]
                print(type(jtype))

                # Add context {
                #       "d:chirpstack:<devEui>"
                #       "s:oyster2avena"
                #     }
                # Check out CID's from interplanetary data

                #context_dict = {}

                device_dict = {"time" : time,
                               "longitudeDeg" : longitude,
                               "latitudeDeg" : latitude,
                               "speedKmph" : speed,
                               "type" : jtype,
                               "context" : {"d" : f"chirpstack:<{dev_ID}>", "s" : "oyster2avena"}
                               }
                
                json_message = json.dumps(device_dict)
                print(type(json_message))
                
                
                await nc.publish(f'machine.{dev_name}.pos', json_message.encode('utf-8'))
                
                # If "exit" is receieved, unsubscribe and terminate the thread.
                if (msg.data.decode() == 'exit'):
                    await sub.unsubscribe()
                    exit = True
        except Exception as e:
            print("Threw on first " + str(e))
            print(traceback.format_exc())
            pass
    await nc.drain
    


async def secondLoop():
    # Establish an initial connection to a NATS server given a port
    # Sub to the desired subject
    # nc = await nats.connect("nats://127.0.0.1:4222")
    #sub = await nc.subscribe("JSON")

    nc = await nats.connect("nats://pipeline.ecn.purdue.edu", user_credentials="/Users/aaravpai/Desktop/TDM.creds")
    sub = await nc.subscribe("machine.*.events.enter")

    exit = False

    #await nc.publish('{"deduplicationId":"44557e00-533f-4806-97e3-b49e1ac1ef61","time":"2024-02-05T21:16:41.969+00:00","deviceInfo":{"tenantId":"52f14cd4-c6f1-4fbd-8f87-4025e1d49242","tenantName":"ChirpStack","applicationId":"11a904ca-ce8f-4145-b931-ddadb0418093","applicationName":"TDM","deviceProfileId":"229ad518-2c86-4233-81fa-e811469b70ce","deviceProfileName":"Digital Matter Oyster","deviceName":"oyster-f300","devEui":"70b3d5705000f300","deviceClassEnabled":"CLASS_A","tags":{}},"devAddr":"0167190b","adr":false,"dr":0,"fCnt":3,"fPort":1,"confirmed":false,"data":"AAAAAAAAAAADAM8=","object":{"longitudeDeg":8.0,"batV":5.175,"headingDeg":0.0,"speedKmph":0.0,"latitudeDeg":5.0,"type":"position","fixFailed":true,"inTrip":true,"manDown":null},"rxInfo":[{"gatewayId":"ac1f09fffe0588e7","uplinkId":16677,"time":"2024-02-05T21:16:40.969533+00:00","timeSinceGpsEpoch":"1391203019.969s","rssi":-31,"snr":13.0,"channel":3,"location":{"latitude":40.42138,"longitude":-86.91566,"altitude":184.0},"context":"Oaezlw==","metadata":{"region_common_name":"US915","region_config_id":"us915_1_DR_1_3"},"crcStatus":"CRC_OK"}],"txInfo":{"frequency":904500000,"modulation":{"lora":{"bandwidth":125000,"spreadingFactor":10,"codeRate":"CR_4_5"}}}}', b'First')

    while(exit == False):
        try:
            await asyncio.sleep(0.01)
            # Every time sub receives a message, enter this for loop
            async for msg in sub.messages:
                # Print the subject and message
                print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
                
                # Load the string received by messages as a JSON file
                # Automatically creates a python dictionary (json.loads)
                json_obj = json.loads(msg.data.decode())

                #dev_ID = json_obj["deviceInfo"]["devEui"]
                #dev_name = ''
                fields = ['polygon','time','name']
                list_of_objs = []

                with open("Curr_polys.csv", 'r') as f:
                    dict_reader = DictReader(f)
                    list_of_objs = list(dict_reader)

                polygon = json_obj["polygon"]
                time = json_obj["time"]
                name = msg.subject.split('.')[1]
                in_poly_dict[name] = 1;
                name_poly_dict[name] = polygon
                with open('Curr_polys.csv', mode='w+', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames = fields, extrasaction='ignore')
                    writer.writeheader()
                    info = {'polygon' : polygon,
                            'time' : time,
                            'name' : name
                            }
                    #new_json = json.dumps(info)
                    #print(type(new_json))
                    #print(info)

                    list_of_objs.append(info)
                    writer.writerows(list_of_objs)
                
                # If "exit" is receieved, unsubscribe and terminate the thread.
                if (msg.data.decode() == 'exit'):
                    await sub.unsubscribe()
                    exit = True
        except Exception as e:
            print("Threw on first " + str(e))
            pass
    await nc.drain

async def thirdLoop():
    # Establish an initial connection to a NATS server given a port
    # Sub to the desired subject
    #nc = await nats.connect("nats://127.0.0.1:4222")
    #sub = await nc.subscribe("JSON")

    nc = await nats.connect("nats://pipeline.ecn.purdue.edu", user_credentials="/Users/aaravpai/Desktop/TDM.creds")
    sub = await nc.subscribe("machine.*.events.exit")

    exit = False

    while(exit == False):
        try:
            await asyncio.sleep(0.01)
            # Every time sub receives a message, enter this for loop
            async for msg in sub.messages:
                # Print the subject and message
                print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
                
                # Load the string received by messages as a JSON file
                # Automatically creates a python dictionary (json.loads)
                json_obj = json.loads(msg.data.decode())

                polygon = json_obj["polygon"]
                dev_name = msg.subject.split(".")[1]
                days = 0
                hours = 0
                minutes = 0
                seconds = 0
                list_of_objs = []
                with open("Curr_polys.csv", 'r') as f:
                    dict_reader = DictReader(f)
                    list_of_objs = list(dict_reader)
                    copy_list = list_of_objs.copy()
                    for i in range (len(copy_list)):
                        obj = copy_list[i]
                        print(obj)
                        if (obj['name'] == msg.subject.split('.')[1]):
                            time_entered = datetime.strptime(obj['time'], "%Y-%m-%dT%H:%M:%S.%f%z")
                            time_exited = datetime.strptime(json_obj['time'], "%Y-%m-%dT%H:%M:%S.%f%z")
                            delta = time_exited - time_entered
                            days = delta.days
                            hours = delta.seconds // 3600
                            minutes = (delta.seconds % 3600) // 60
                            seconds = (delta.seconds % 3600) % 60
                            del list_of_objs[i]
                            in_poly_dict[dev_name] = 2
                            break
                
                fields = ['polygon','time','name']
                with open('Curr_polys.csv', mode='w+', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames = fields, extrasaction='ignore')
                    writer.writeheader()
                    print(list_of_objs)
                    writer.writerows(list_of_objs)


                message_dict = {"polygon" : polygon,
                               "time elapsed" : f'{days} days, {hours} hours, {minutes} minutes, {seconds} seconds',
                               }
                
                json_message = json.dumps(message_dict)
                
                
                await nc.publish(f'machine.{dev_name}.timeElapsed', json_message.encode('utf-8'))
                
                # If "exit" is receieved, unsubscribe and terminate the thread.
                if (msg.data.decode() == 'exit'):
                    await sub.unsubscribe()
                    exit = True
        except Exception as e:
            print("Threw on first " + str(e))
            pass
    await nc.drain

async def fourthLoop():
    # Establish an initial connection to a NATS server given a port
    # Sub to the desired subject
    #nc = await nats.connect("nats://127.0.0.1:4222")
    #sub = await nc.subscribe("JSON")

    nc = await nats.connect("nats://pipeline.ecn.purdue.edu", user_credentials="/Users/aaravpai/Desktop/TDM.creds")
    sub = await nc.subscribe("polygon.*.events.enter")

    exit = False

    #await nc.publish('{"deduplicationId":"44557e00-533f-4806-97e3-b49e1ac1ef61","time":"2024-02-05T21:16:41.969+00:00","deviceInfo":{"tenantId":"52f14cd4-c6f1-4fbd-8f87-4025e1d49242","tenantName":"ChirpStack","applicationId":"11a904ca-ce8f-4145-b931-ddadb0418093","applicationName":"TDM","deviceProfileId":"229ad518-2c86-4233-81fa-e811469b70ce","deviceProfileName":"Digital Matter Oyster","deviceName":"oyster-f300","devEui":"70b3d5705000f300","deviceClassEnabled":"CLASS_A","tags":{}},"devAddr":"0167190b","adr":false,"dr":0,"fCnt":3,"fPort":1,"confirmed":false,"data":"AAAAAAAAAAADAM8=","object":{"longitudeDeg":8.0,"batV":5.175,"headingDeg":0.0,"speedKmph":0.0,"latitudeDeg":5.0,"type":"position","fixFailed":true,"inTrip":true,"manDown":null},"rxInfo":[{"gatewayId":"ac1f09fffe0588e7","uplinkId":16677,"time":"2024-02-05T21:16:40.969533+00:00","timeSinceGpsEpoch":"1391203019.969s","rssi":-31,"snr":13.0,"channel":3,"location":{"latitude":40.42138,"longitude":-86.91566,"altitude":184.0},"context":"Oaezlw==","metadata":{"region_common_name":"US915","region_config_id":"us915_1_DR_1_3"},"crcStatus":"CRC_OK"}],"txInfo":{"frequency":904500000,"modulation":{"lora":{"bandwidth":125000,"spreadingFactor":10,"codeRate":"CR_4_5"}}}}', b'First')

    while(exit == False):
        try:
            await asyncio.sleep(0.01)
            # Every time sub receives a message, enter this for loop
            async for msg in sub.messages:
                # Print the subject and message
                print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
                
                # Load the string received by messages as a JSON file
                # Automatically creates a python dictionary (json.loads)
                json_obj = json.loads(msg.data.decode())

                #dev_ID = json_obj["deviceInfo"]["devEui"]
                #dev_name = ''
                fields = ['polygon','time','name']
                list_of_objs = []

                with open("Poly_events.csv", 'r') as f:
                    dict_reader = DictReader(f)
                    list_of_objs = list(dict_reader)

                name= json_obj["machine"]
                time = json_obj["time"]
                polygon = msg.subject.split('.')[1]
                with open('Poly_events.csv', mode='w+', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames = fields, extrasaction='ignore')
                    writer.writeheader()
                    info = {'polygon' : polygon,
                            'time' : time,
                            'name' : name
                            }
                    #new_json = json.dumps(info)
                    #print(type(new_json))
                    #print(info)

                    list_of_objs.append(info)
                    writer.writerows(list_of_objs)
                
                # If "exit" is receieved, unsubscribe and terminate the thread.
                if (msg.data.decode() == 'exit'):
                    await sub.unsubscribe()
                    exit = True
        except Exception as e:
            print("Threw on first " + str(e))
            pass
    await nc.drain

async def fifthLoop():

    # Establish an initial connection to a NATS server given a port
    # Sub to the desired subject
    #nc = await nats.connect("nats://127.0.0.1:4222")
    #sub = await nc.subscribe("JSON")

    nc = await nats.connect("nats://pipeline.ecn.purdue.edu", user_credentials="/Users/aaravpai/Desktop/TDM.creds")
    sub = await nc.subscribe("polygon.*.events.exit")

    exit = False

    while(exit == False):
        try:
            await asyncio.sleep(0.01)
            # Every time sub receives a message, enter this for loop
            async for msg in sub.messages:
                # Print the subject and message
                print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
                
                # Load the string received by messages as a JSON file
                # Automatically creates a python dictionary (json.loads)
                json_obj = json.loads(msg.data.decode())

                dev_name = json_obj["machine"]
                polygon = msg.subject.split(".")[1]
                days = 0
                hours = 0
                minutes = 0
                seconds = 0
                list_of_objs = []
                with open("Poly_events.csv", 'r') as f:
                    dict_reader = DictReader(f)
                    list_of_objs = list(dict_reader)
                    copy_list = list_of_objs.copy()
                    for i in range (len(copy_list)):
                        obj = copy_list[i]
                        print(type(obj))
                        if (obj['polygon'] == msg.subject.split('.')[1]):
                            time_entered = datetime.strptime(obj['time'], "%Y-%m-%dT%H:%M:%S.%f%z")
                            time_exited = datetime.strptime(json_obj['time'], "%Y-%m-%dT%H:%M:%S.%f%z")
                            delta = time_exited - time_entered
                            days = delta.days
                            hours = delta.seconds // 3600
                            minutes = (delta.seconds % 3600) // 60
                            seconds = (delta.seconds % 3600) % 60
                            del list_of_objs[i]
                            break
                
                fields = ['polygon','time','name']
                with open('Poly_events.csv', mode='w+', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames = fields)
                    writer.writeheader()
                    #for entry in list_of_objs:
                       #print(type(entry))
                        #writer.writerows(entry)
                    writer.writerows(list_of_objs)


                message_dict = {"machine" : dev_name,
                               "time elapsed" : f'{days} days, {hours} hours, {minutes} minutes, {seconds} seconds',
                               }
                
                json_message = json.dumps(message_dict)
                
                
                await nc.publish(f'polygon.{polygon}.timeElapsed', json_message.encode('utf-8'))
                
                # If "exit" is receieved, unsubscribe and terminate the thread.
                if (msg.data.decode() == 'exit'):
                    await sub.unsubscribe()
                    exit = True
        except Exception as e:
            print("Threw on first " + str(e))
            pass
    await nc.drain

in_poly_dict = {}
name_poly_dict = {}

async def sixthLoop():
    # Establish an initial connection to a NATS server given a port
    # Sub to the desired subject
    #nc = await nats.connect("nats://127.0.0.1:4222")
    #sub = await nc.subscribe("JSON")

    nc = await nats.connect("nats://pipeline.ecn.purdue.edu", user_credentials="/Users/aaravpai/Desktop/TDM.creds")
    sub = await nc.subscribe('machine.*.pos')
    

    exit = False

    while(exit == False):
        try:
            await asyncio.sleep(0.01)
            # Every time sub receives a message, enter this for loop
            async for msg in sub.messages:
                # Print the subject and message
                print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
                
                # Load the string received by messages as a JSON file
                # Automatically creates a python dictionary (json.loads)
                json_obj = json.loads(msg.data.decode())

                dev_name = msg.subject.split('.')[1]
                print(in_poly_dict)
                if (in_poly_dict[dev_name] == 1):
                    print("top 1")
                    avg_speed = 0.0
                    hours = 0
                    minutes = 0
                    seconds = 0
                    entered = 0

                    list_of_objs = []
                    with open("curr_speeds.csv", 'r') as f:
                        dict_reader = DictReader(f)
                        list_of_objs = list(dict_reader)
                        copy_list = list_of_objs.copy()
                        for i in range (len(copy_list)):
                            obj = copy_list[i]
                            print(obj)
                            if (obj['name'] == dev_name):
                                avg_speed = obj['speed']
                                # Get the time stamp of the last available data from this machine
                                l_a_time = datetime.strptime(obj['time_stamp'], "%Y-%m-%dT%H:%M:%S.%f%z")
                                # Find the difference between the last available time and the timestamp of the current data
                                time_delta = datetime.strptime(json_obj['time'], "%Y-%m-%dT%H:%M:%S.%f%z") - l_a_time
                                # Get the previous total time spent with the stored average speed
                                # The value is stored in the CSV as a timedelta object
                                p_total_time = timedelta(hours=float(obj['delt'].split(':')[0]), minutes=float(obj['delt'].split(':')[1]), seconds=float(obj['delt'].split(':')[2]))

                                # Find the total distance traveled before the new data point
                                p_total_dist = float(p_total_time.total_seconds() / 3600) * avg_speed

                                # Find the average speed between our previous avg and the new speed data

                                avg_speed = (avg_speed + json_obj['speedKmph']) / 2
                                
                                # Find the total distance traveled in the new time interval
                                print("or here")
                                new_total_dist = (time_delta.total_seconds / 3600) * avg_speed

                                # Find an estimate for the total distance traveled (time + average speed)
                                total_dist = p_total_dist + new_total_dist
                                total_time = ((p_total_time + time_delta).total_seconds()) / 3600
                                hours = ((p_total_time + time_delta).total_seconds()) // 3600
                                minutes = ((p_total_time + time_delta).total_seconds() % 3600) // 60
                                seconds = ((p_total_time + time_delta).total_seconds() % 3600) % 60
                                avg_speed = total_dist / total_time
                                entered = 1
                                del list_of_objs[i]
                                break
                    if (entered == 0):
                        avg_speed = json_obj['speedKmph']
                    
                    time_stamp = json_obj['time']
                    list_of_objs.append({'name':dev_name, 'speed':avg_speed, 'time_stamp':time_stamp, 'delt':f'{hours}:{minutes}:{seconds}'})
                    
                    fields = ['name','speed','time_stamp','delt']
                    with open('curr_speeds.csv', mode='w+', newline='') as file:
                        writer = csv.DictWriter(file, fieldnames = fields, extrasaction='ignore')
                        writer.writeheader()
                        print(list_of_objs)
                        writer.writerows(list_of_objs)
                elif (in_poly_dict[dev_name] == 2):
                    print("top 2")
                    list_of_objs = []
                    spd = 0.0
                    with open("curr_speeds.csv", 'r') as f:
                        dict_reader = DictReader(f)
                        list_of_objs = list(dict_reader)
                        copy_list = list_of_objs.copy()
                        for i in range (len(copy_list)):
                            obj = copy_list[i]
                            if (obj['name'] == dev_name):
                                speed = obj['speed']
                                del list_of_objs[i]
                                break
                    in_poly_dict[dev_name] = 0
                    name_poly_dict.pop(dev_name)
                    message_dict = {"name":dev_name, "speed":speed, "polygon":name_poly_dict[dev_name]}
                    json_message = json.dumps(message_dict)
                    await nc.publish(f'machine.{dev_name}.avgSpeed', json_message.encode('utf-8'))
                # If "exit" is receieved, unsubscribe and terminate the thread.
                if (msg.data.decode() == 'exit'):
                    await sub.unsubscribe()
                    exit = True
        except Exception as e:
            print("Threw on first " + str(e))
            print(traceback.format_exc())
            pass
    await nc.drain

# Currently unused
#async def main():
   # f1 = loop.create_task(firstLoop)
   # f2 = loop.create_task(secondLoop)
   # await asyncio.wait([f1,f2])

loop = asyncio.new_event_loop() 
asyncio.set_event_loop(loop)
asyncio.ensure_future(firstLoop()) 
asyncio.ensure_future(thirdLoop())
asyncio.ensure_future(fourthLoop())
asyncio.ensure_future(fifthLoop())
asyncio.ensure_future(secondLoop())
#asyncio.ensure_future(sixthLoop()) 
loop.run_forever()
