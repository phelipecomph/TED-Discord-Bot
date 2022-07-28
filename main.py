import discord
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler


from feature_results import schedule_results
from feature_ferias import get_user
from keep_alive import keep_alive

intents = discord.Intents.default()
intents.members = True
client = discord.Client(intents=intents)



@client.event
async def on_ready():
  print(f'Logamos com sucesso como {client.user}')
  scheduler = AsyncIOScheduler()
  schedule_results(scheduler,client)
  
  scheduler.start()
  

@client.event
async def on_message(message):
  if message.author == client.user:
    return
  if message.content.startswith('!TED'):
    #await message.channel.send('<@171650082523971585>')
    await message.channel.send(f"{get_user(client, 'Adri#4424').mention}")



keep_alive()
client.run(os.environ['DISCORD_TOKEN'])
