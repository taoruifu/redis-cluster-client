# Redis Cluster Client

This is a Node.js version of [Redis-rb-cluster](https://github.com/antirez/redis-rb-cluster).In addition,this version add read from slave nodes without redirect to master nodes.

# Create instance
In oreder to create a new Redis Cluster instance use:  

    var RedisCluster = require('redis-cluster-client');
    var start_nodes = [{host:'127.0.0.1',port:6805},{host:'127.0.0.1',port:6806}]
    rcluster = RedisCluster.createClient(start_nodes,{role:'master'});  //write cluster

    rcluster = RedisCluster.createClient(start_nodes,{role:'slave'});  //readonly cluster

# Sending commands

    rcluster.get("foo",function(err,reply){
            //process err and reply
    });