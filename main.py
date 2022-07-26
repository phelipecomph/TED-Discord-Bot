import discord
from discord.ext import tasks
import os
import datetime
import time
import calendar
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from feature_parciais import get_mpe_currentmonth_text,get_mpe_lastmonth_text
from keep_alive import keep_alive

client = discord.Client()

id_channel_mpe = 789570138600374292

@client.event
async def on_ready():
  print(f'Logamos com sucesso como {client.user}')
  scheduler = AsyncIOScheduler()
  
  #Time = UTF (some 3)
  scheduler.add_job(send_mpe_result, CronTrigger(hour="11", minute="0", second="0")) 
  
  scheduler.start()
  

@client.event
async def on_message(message):
  if message.author == client.user:
    return
  if message.content.startswith('!TED'):
    await message.channel.send(get_mpe_lastmonth_text())

async def send_mpe_result():
  now = datetime.datetime.now()
  await client.wait_until_ready()
  c = client.get_channel(id_channel_mpe)
  if now.day() <= 4 and now.weekday() in [0,3]: #Segunda ou Quinta Até dia 4 
    await c.send(get_mpe_lastmonth_text())
  if now.weekday() in [0,3]: #Segunda ou Quinta
    await c.send(get_mpe_currentmonth_text())

keep_alive()
client.run(os.environ['DISCORD_TOKEN'])
