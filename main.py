import discord
import os
from feature_parciais import test_feature
from keep_alive import keep_alive

client = discord.Client()

@client.event
async def on_ready():
  print(f'Logamos com sucesso como {client.user}')

@client.event
async def on_message(message):
  if message.author == client.user:
    return
  if message.content.startswith('$hello'):
    await message.channel.send('Hello!')

test_feature()
#keep_alive()
#client.run(os.environ['DISCORD_TOKEN'])
