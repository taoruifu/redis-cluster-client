redis = require('redis')
client = redis.createClient(6810,'192.168.33.10');

client.set 'abc',123,(err,reply)->
    console.log err if err
    console.log reply if reply
