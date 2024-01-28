from random import randint
from paho.mqtt.client import Client
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from os import mkdir
from os.path import exists

# prepare data 
data = {}

def saveData():
  now = datetime.now()
  date = now.strftime("%Y%m%d")
  time = now.strftime("%H%M%S")

  if not exists(date): mkdir(date)
  with open(date+"/"+time+".txt", "w") as f:
    for topic, value in data.items():
      f.write(topic + " = " + value + "\n")

# prepare scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(saveData, CronTrigger(minute="0,10,20,30,40,50"))
scheduler.start()

broker = 'test.mosquitto.org'
port = 1883
topic = "OpenDTU-A60F1C/#"
# Generate a Client ID with the subscribe prefix.
client_id = f'subscribe-{randint(0, 100)}'


def on_connect(client, userdata, flags, rc):
  if rc == 0:
    print("Connected to MQTT Broker!")
  else:
    print("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, msg):
  topic = msg.topic
  value = msg.payload.decode()
  print(f"{datetime.now().isoformat()}: {topic} = {value}")

  data[topic] = value


client = Client(client_id)
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, port)
client.subscribe(topic)
client.loop_forever()
