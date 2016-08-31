import redis
# rds = redis.StrictRedis(port=6379)
rds = redis.StrictRedis(port=8080)

print ">>>", rds.execute_command("ECHO", "BAR")

assert ["BAR"] == rds.execute_command("ECHO", "BAR")
assert ["BAR", "BAZ"] == rds.execute_command("ECHO", "BAR", "BAZ")
assert ["BAR" * 2000] == rds.execute_command("ECHO", "BAR" * 2000)

assert ["BAR\nBAR"] == rds.execute_command("ECHO", "BAR\nBAR")

# pipe = rds.pipeline()
# pipe.execute_command('ECHO', 'bar')
# print "sent execute"
# values = pipe.execute(raise_on_error=True)
# print values