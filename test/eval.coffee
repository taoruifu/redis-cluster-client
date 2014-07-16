RedisCluster = require('../index')
rcluster = RedisCluster.createClient([{host:'192.168.33.10',port:6805},{host:'192.168.33.10',port:6806}],{role:'master'});


rcluster.on 'ready',()->
    console.log rcluster.ready
    setInterval ()->
        s = new Date().getTime();
        rcluster.eval "return KEYS[1]",1,5678,(err,reply)->
            return console.log("e:"+err.toString()) if err
            console.log(reply.toString()) if reply
            console.log("consume "+(new Date().getTime() - s));
    ,20
