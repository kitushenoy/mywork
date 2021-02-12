from time import sleep
from json import dumps
from kafka import KafkaProducer
from random import randrange
import random
import datetime,time
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
for e in range(100):
    data = {'timestamp':datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),'deviceid' :random.randrange(5) ,'sensorid':random.randint(0,1),'value':randrange(10)}
    print "data generated :",data
    producer.send('devicedata', value=data)
    sleep(0.5)

