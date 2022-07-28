def get_user(ctx, username):
  #user = ctx.users
  for guild in ctx.guilds:
    for member in guild.members:
      if f'{member.name}#{member.discriminator}' == username:
        match = member
        return match