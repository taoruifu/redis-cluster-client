RedisCluster = require('../index')
rcluster = RedisCluster.createClient([{host:'192.168.33.10',port:6801},{host:'192.168.33.10',port:6803}]);

last = no
setTimeout ()->
  console.log 'ready'
  rcluster.get "__lat__",(err,reply)->
    console.log reply
    return console.log("can not get last index:"+err.toString()) if err
    last = reply
    last = 0 unless last
    last = parseInt last
    t = last

    console.log last
    setInterval  ()->
          last = last + 1
          do(last)->
              start = (new Date()).getTime()
              rcluster.set "moo#{last}",last,(err,reply)->
                  console.log "err when set:#{err.toString()}" if err
                  console.log "set moo#{last} #{reply}"
              rcluster.set "__lat__",last,(err,reply)->
                  console.log "err when set:#{err.toString()}" if err
                  console.log "set __lat__#{last} #{reply}"
              rcluster.get "moo#{last}",(err,reply)->
                  console.log "err when get:#{err.toString()}" if err
                  console.log "get moo#{last} #{reply}"
                  console.log "consume #{(new Date()).getTime() - start}"
              ###
              rcluster.set "abc#{last}",last,(err,reply)->
                  console.log "err when set:#{err.toString()}" if err
                  console.log "set abc#{last} #{reply}"
              rcluster.get "abc#{last}",(err,reply)->
                  console.log "err when get:#{err.toString()}" if err
                  console.log "get abc#{last} #{reply}"
                  console.log "consume #{(new Date()).getTime() - start}"
              ###
         ,50

     setInterval ()->
         for c of rcluster.connections
             console.log c+" ----------"
         console.log ' '
        ,5000
 ,5000
