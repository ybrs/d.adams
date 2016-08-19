import redis
rds = redis.StrictRedis(port=8080)

assert ["BAR"] == rds.execute_command("ECHO", "BAR")
assert ["BAR", "BAZ"] == rds.execute_command("ECHO", "BAR", "BAZ")
assert ["BAR" * 2000] == rds.execute_command("ECHO", "BAR" * 2000)

assert ["BAR\nBAR"] == rds.execute_command("ECHO", "BAR\nBAR")