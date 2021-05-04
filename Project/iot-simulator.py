
import pandas as pd
import json
import datetime 
from datetime import datetime
from datetime import time
from time import sleep 

import paho.mqtt.client as mqtt


topic="data"

packet_interval=1

data={
    "datetime":1446476777,
    "assetId":"334242",
    "energy_re":43,
    "energy_ac":33,
    "pf":1,
    "freq":50,
    "voltage":230,
    "current":21,
    "power_re":43,
    "power_ac":33,
}

ratings={
    "ac_pow":{
        "rating":3500,
        "number":1
    },
    "fan":70,
    "light":20,
    "bulb":10,
    "tv":80,
    "fridge":150,
    "mixer_grinder":500
}
ratings
pf=0.99
freq=49.99
total_en=0



rate0_to_7_59_and_23={
    "ac":{
        "rating":3500,
        "number":1
    },
    "fan":{
        "rating":70,
        "number":2
    },
    "light":{
        "rating":20,
        "number":0
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":0
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":0
    }
}

rate8_to_9_59={
    "ac":{
        "rating":3500,
        "number":0
    },
    "fan":{
        "rating":70,
        "number":2
    },
    "light":{
        "rating":20,
        "number":1
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":0
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":0
    }
}

rate10_to_10_59_and_16_59={
    "ac":{
        "rating":3500,
        "number":0
    },
    "fan":{
        "rating":70,
        "number":2
    },
    "light":{
        "rating":20,
        "number":0
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":0
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":0
    }
}


rate11_to_11_59={
    "ac":{
        "rating":3500,
        "number":0
    },
    "fan":{
        "rating":70,
        "number":2
    },
    "light":{
        "rating":20,
        "number":0
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":0
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":1
    }
}



rate12_to_15_59={
    "ac":{
        "rating":3500,
        "number":0
    },
    "fan":{
        "rating":70,
        "number":2
    },
    "light":{
        "rating":20,
        "number":0
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":1
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":0
    }
}



rate17_to_18_59={
    "ac":{
        "rating":3500,
        "number":0
    },
    "fan":{
        "rating":70,
        "number":0
    },
    "light":{
        "rating":20,
        "number":0
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":0
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":0
    }
}


rate19_to_19_59={
    "ac":{
        "rating":3500,
        "number":0
    },
    "fan":{
        "rating":70,
        "number":1
    },
    "light":{
        "rating":20,
        "number":2
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":1
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":0
    }
}


rate20_to_21_59={
    "ac":{
        "rating":3500,
        "number":0
    },
    "fan":{
        "rating":70,
        "number":2
    },
    "light":{
        "rating":20,
        "number":2
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":1
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":0
    }
}



rate22_to_22_59={
    "ac":{
        "rating":3500,
        "number":1
    },
    "fan":{
        "rating":70,
        "number":2
    },
    "light":{
        "rating":20,
        "number":2
    },
    "bulb":{
        "rating":10,
        "number":0
    },
    "tv":{
        "rating":80,
        "number":1
    },
    "fridge":{
        "rating":150,
        "number":1
    },
    "mixer_grinder":{
        "rating":500,
        "number":0
    }
}



now = datetime.now()
cur_time=now.strftime("%H:%M:%S")

cur_time=datetime.strptime(cur_time, '%H:%M:%S').time()
cur_time


now = datetime.strptime(datetime.now().strftime("%H:%M:%S"), '%H:%M:%S').time()
now



def time_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end


import random
# import time
def prepare_pkt(ratings_data):

    data={
        "id":"607a81a44edce7001d469d2c",
        "timestamp":round(datetime.now().timestamp()),
        "sigStrength":11,
        "assets":[{
            "assetId":"607a82ad4edce7001d469d2e",
            "parameters":[
                {},{},{},{},{}
            ]  
        }]
    }
    ac_pw=ratings_data['ac']['rating']*ratings_data['ac']['number']
    fan_pw=ratings_data['fan']['rating']*ratings_data['fan']['number']
    light_pw=ratings_data['light']['rating']*ratings_data['light']['number']
    bulb_pw=ratings_data['bulb']['rating']*ratings_data['bulb']['number']
    tv_pw=ratings_data['tv']['rating']*ratings_data['tv']['number']
    fridge_pw=ratings_data['fridge']['rating']*ratings_data['fridge']['number']
    mixer_grinder_pw=ratings_data['mixer_grinder']['rating']*ratings_data['mixer_grinder']['number']
    data['assets'][0]['parameters'][0]['value']=ac_pw+fan_pw+light_pw+bulb_pw+tv_pw+fridge_pw+mixer_grinder_pw
    data['assets'][0]['parameters'][1]['value']=round(random.uniform(230.0, 239.99),2)
    data['assets'][0]['parameters'][2]['value']=round(random.uniform(0.95, 1.00),2)
    data['assets'][0]['parameters'][3]['value']=round(random.uniform(49.900, 50.000),2)
    data['assets'][0]['parameters'][4]['value']=round(data['assets'][0]['parameters'][0]['value']/(pf*data['assets'][0]['parameters'][1]['value']),2)
    
    data['assets'][0]['parameters'][0]['parameterId']="607a857b4edce7001d469d31"
    data['assets'][0]['parameters'][1]['parameterId']="607a857b4edce7001d469d32"
    data['assets'][0]['parameters'][2]['parameterId']="607a857b4edce7001d469d33"
    data['assets'][0]['parameters'][3]['parameterId']="607a857b4edce7001d469d34"
    data['assets'][0]['parameters'][4]['parameterId']="607a857b4edce7001d469d35"
    
    return data

    

def run_simulator():
    now = datetime.now().strftime("%H:%M:%S")
    curr=datetime.strptime(now, '%H:%M:%S').time()

    data={}

    if(time_in_range(time(0, 0, 0),time(0, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)
    elif(time_in_range(time(1, 0, 0),time(1, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)
    elif(time_in_range(time(2, 0, 0),time(2, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)
    elif(time_in_range(time(3, 0, 0),time(3, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)
    elif(time_in_range(time(4, 0, 0),time(4, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)
    elif(time_in_range(time(5, 0, 0),time(5, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)
    elif(time_in_range(time(6, 0, 0),time(6, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)
    elif(time_in_range(time(7, 0, 0),time(7, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)
    elif(time_in_range(time(8, 0, 0),time(8, 59, 59),curr)):
        data=prepare_pkt(rate8_to_9_59)
    elif(time_in_range(time(9, 0, 0),time(9, 59, 59),curr)):
        data=prepare_pkt(rate8_to_9_59)       
    elif(time_in_range(time(10, 0, 0),time(10, 59, 59),curr)):
        data=prepare_pkt(rate10_to_10_59_and_16_59)
    elif(time_in_range(time(11, 0, 0),time(11, 59, 59),curr)):
        data=prepare_pkt(rate11_to_11_59)
    elif(time_in_range(time(12, 0, 0),time(12, 59, 59),curr)):
        data=prepare_pkt(rate12_to_15_59)
    elif(time_in_range(time(13, 0, 0),time(13, 59, 59),curr)):
        data=prepare_pkt(rate12_to_15_59)
    elif(time_in_range(time(14, 0, 0),time(14, 59, 59),curr)):
        data=prepare_pkt(rate12_to_15_59)
    elif(time_in_range(time(15, 0, 0),time(15, 59, 59),curr)):
        data=prepare_pkt(rate12_to_15_59)
    elif(time_in_range(time(16, 0, 0),time(16, 59, 59),curr)):
        data=prepare_pkt(rate10_to_10_59_and_16_59)
    elif(time_in_range(time(17, 0, 0),time(17, 59, 59),curr)):
        data=prepare_pkt(rate17_to_18_59)
    elif(time_in_range(time(18, 0, 0),time(18, 59, 59),curr)):
        data=prepare_pkt(rate17_to_18_59)
    elif(time_in_range(time(19, 0, 0),time(19, 59, 59),curr)):
        data=prepare_pkt(rate19_to_19_59)
    elif(time_in_range(time(20, 0, 0),time(20, 59, 59),curr)):
        data=prepare_pkt(rate20_to_21_59)
    elif(time_in_range(time(21, 0, 0),time(21, 59, 59),curr)):
        data=prepare_pkt(rate20_to_21_59)
    elif(time_in_range(time(22, 0, 0),time(22, 59, 59),curr)):
        data=prepare_pkt(rate22_to_22_59)
    elif(time_in_range(time(23, 0, 0),time(23, 59, 59),curr)):
        data=prepare_pkt(rate0_to_7_59_and_23)

    return data

# function to add to JSON
def write_json(data, filename='data.json'):
    with open(filename,'w') as f:
        json.dump(data, f, indent=4)






# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("mqtt.eclipse.org", 1883, 60)

gen=run_simulator()

publish(topic, gen)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()



# # while(True):
# for x in range(0,100):
#     gen=run_simulator()

#     # print((gen))

#     with open('data.json') as json_file:
#         data = json.load(json_file)
        

#         # appending data to emp_details 
#         data.append(gen)
      
#     write_json(data)
#     sleep(1)
