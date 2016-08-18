import redis
import time

conn = redis.StrictRedis(port=8080)

conn.execute_command("CLIENTID", "python-client-1")

p = conn.pubsub()
p.subscribe(["jobs"])
conn.publish("jobs", "hello - %s" % time.time())
for i in p.listen():
    print ">>>", i