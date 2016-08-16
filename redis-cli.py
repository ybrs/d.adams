import redis

conn = redis.StrictRedis(port=8080)
while True:
    for i in conn.blpop(["hello", "bar"]):
        print ">>>", i