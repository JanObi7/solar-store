from random import randint
from paho.mqtt.client import Client
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from os import mkdir
from os.path import exists
from time import sleep

def receiveData():
  # init data
  data = {}

  # prepare timstamp
  now = datetime.now()
  date = now.strftime("%Y%m%d")
  time = now.strftime("%H%M%S")

  def on_connect(client, userdata, flags, rc):
    if rc == 0:
      print("Connected to MQTT Broker!")
    else:
      print("Failed to connect, return code %d\n", rc)

  def on_disconnect(client, userdata, rc):
    if rc == 0:
      print("Disconnected from MQTT Broker!")
    else:
      print("Failed to disconnect, return code %d\n", rc)

  def on_message(client, userdata, msg):
    topic = msg.topic
    value = msg.payload.decode()
    data[topic] = value

    # alive print
    if topic.endswith("/dtu/uptime"):
      print(f"{datetime.now().isoformat()}: {topic} = {value}")

  # setup mqtt client
  broker = 'test.mosquitto.org'
  port = 1883
  topic = "OpenDTU-A60F1C/#"
  # Generate a Client ID with the subscribe prefix.
  client_id = f'subscribe-{randint(0, 100)}'

  client = Client(client_id)
  client.on_connect = on_connect
  client.on_disconnect = on_disconnect
  client.on_message = on_message

  # connect
  print("connect to broker")
  client.connect(broker, port)
  client.loop_start()

  # subscrib and wait
  client.subscribe(topic)
  print("wait 30 seconds")
  sleep(30)

  # disconnect
  print("disconnect from broker")
  client.disconnect()
  client.loop_stop()

  # save data to file
  if not exists(date): mkdir(date)
  with open(date+"/"+time+".txt", "w") as f:
    for topic, value in data.items():
      f.write(topic + " = " + value + "\n")

# prepare scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(receiveData, CronTrigger(minute="0,10,20,30,40,50"))

# start and wait forever
scheduler.start()
while True:
  sleep(1)
