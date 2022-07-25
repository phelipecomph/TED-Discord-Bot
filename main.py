import discord
import os

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


#keep_alive()
#client.run(os.environ['DISCORD_TOKEN'])