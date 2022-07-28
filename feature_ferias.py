import pandas as pd
import datetime
from apscheduler.triggers.cron import CronTrigger

id_channel = 789468494298742794


def schedule_ferias(scheduler,clx):
  
  global client
  client = clx
  scheduler.add_job(send_ferias,  CronTrigger(hour="17", minute="00", second="00")) #14:00:00
  print('FERIAS Adicionado ao Cronograma com Sucesso')

async def send_ferias():
  now = datetime.datetime.now()
  await client.wait_until_ready()
  c = client.get_channel(id_channel)
  if now.weekday() in [1, 3]: #Terça e Sexta
    await c.send(get_ferias_text())

def get_ferias_text(days=15):
  df = download_xslx()
  df = df.dropna(subset=['Inicio', 'Retorno'])
  df['Inicio'] = pd.to_datetime(df['Inicio'], infer_datetime_format=True)
  df['Retorno'] = pd.to_datetime(df['Retorno'], infer_datetime_format=True)
  df = df.sort_values(by=['Inicio'])
  text = ''
  lista = []
  for index, row in df.iterrows():
      if datetime.datetime.now() < row['Inicio'] < datetime.datetime.now() + datetime.timedelta(days=days):
        lista.append([row['Inicio'],row['Retorno'],row['Nome'],row['Discord Tag']])
  
  if len(lista)>0:text += 'Hey @everyone! Já se prepararam para as férias desses TEDers?\n'
  for item in lista: text += f'{get_user(client,item[3]).mention} sai dia: {item[0].day}/{item[0].month} | e volta dia: {item[1].day}/{item[1].month}\n'
  
  lista = []
  for index, row in df.iterrows():
      if datetime.datetime.now() < row['Retorno'] < datetime.datetime.now() + datetime.timedelta(days=days):
        lista.append([row['Inicio'],row['Retorno'],row['Nome'],row['Discord Tag']])
  
  if len(text) == 0 and len(lista)>0: text += 'Hey @everyone já podem comemorar, olha quem ja ja ta voltando de ferias!\n'
  elif len(lista)>0:text += '\n\nMas fiquem tranquilos que esses já estão voltando\n'
  for item in lista: text += f'{get_user(client,item[3]).mention} voltando dia: {item[1].day}/{item[1].month}\n'
  return text

def get_user(ctx, username):
  #user = ctx.users
  for guild in client.guilds:
    for member in guild.members:
      if f'{member.name}#{member.discriminator}' == username:
        match = member
        return match

def download_xslx():
  return pd.read_csv('ferias.csv')
 