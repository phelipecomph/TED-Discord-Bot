import discord
from discord.ext import tasks
import os
import datetime
import time
import calendar
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from feature_parciais import get_mpe_currentmonth_text
from keep_alive import keep_alive

client = discord.Client()

id_channel_skywalker = 999859270608769117

@client.event
async def on_ready():
  print(f'Logamos com sucesso como {client.user}')
  scheduler = AsyncIOScheduler()
  
  #Time = UTF (some 3)
  scheduler.add_job(send_mpe_parcial, CronTrigger(hour="13", minute="0", second="0")) 
  
  scheduler.start()
  

@client.event
async def on_message(message):
  if message.author == client.user:
    return
  if message.content.startswith('!TED'):
    await message.channel.send(test_feature())

async def send_mpe_parcial():
  await client.wait_until_ready()
  c = client.get_channel(id_channel_skywalker)
  await c.send(get_mpe_currentmonth_text())

keep_alive()
client.run(os.environ['DISCORD_TOKEN'])
