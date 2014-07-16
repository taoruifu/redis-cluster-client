RedisCluster = require('../index')
rcluster = RedisCluster.createClient([{host:'192.168.33.10',port:6801},{host:'192.168.33.10',port:6803}]);
cache = {}
reads=0
failed_reads=0
writes=0
failed_writes=0
lost_writes=0
not_ack_writes=0

genkey = ()->
    seed = Math.random()
    prefix = ['consist',new Date().getSeconds()].join '|'
    if seed > 0.5
        return prefix+"key_"+parseInt(1000*seed)
    else
        return prefix+"key_"+parseInt(1000*seed)

check_consist = (key,value)->
    expected = cache[key]
    return unless expected
    expected = parseInt expected
    value = parseInt value
    if expected > value
        lost_writes = lost_writes + expected - value
        console.log "------------->--------------"
    else if expected < value
        not_ack_writes = not_ack_writes + value - expected
        console.log "-------------<--------------"
output = ()->
    report = "#{reads} R (#{failed_reads} err) | "
    report = report + "#{writes} W (#{failed_writes} err) | "
    report = report + "#{lost_writes} lost | " if lost_writes > 0
    report = report + "#{not_ack_writes} notack | " if not_ack_writes > 0
    console.log report

setInterval ()->
    output()
   ,1000
rcluster.on 'ready',()->
    setInterval  ()->
          key = genkey()
          rcluster.get key,(err,reply)->
              if err
                  failed_reads = failed_reads + 1
                  return console.log "get key #{key} :#{err.toString()}"
              #console.log "get key #{key} #{reply}" if reply
              check_consist key,reply
              reads = reads + 1
          rcluster.incr key,(err,reply)->
              if err
                  failed_writes = failed_writes + 1
                  console.log "err when set:#{err.toString()}" 
              #console.log "incr #{key} #{reply}"
              else
                  cache[key] = reply
              writes = writes + 1
         ,50

