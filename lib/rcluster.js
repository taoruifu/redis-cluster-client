var crc16 = require('./crc16'),
    redis = require('redis'),
    async = require('async'),
    util = require('util'),
    events = require('events'),
    commands = require('./commands');

function RedisCluster(startup_nodes){
    if (!(this instanceof RedisCluster)) {
        return new RedisCluster(startup_nodes);
    }

    this.startup_nodes = startup_nodes || [{port:6379,host:'127.0.0.1'}];
}

util.inherits(RedisCluster,events.EventEmitter);

RedisCluster.prototype.createClient = function(opt){
    this.opt = opt||{};
    this.role = this.opt.role || 'master';
    this.slots = [];       //slot num(0~16383) ---> [{host:*.*.*.*,port:xyz,name:"host:port"}]
    this.nodes = [];       //id --->{address:{host,port,name},role,slot} 
    this.connections = {}; // "host:port"--->connection socket

    this.RedisClusterHashSlots = 16384;
    this.RedisClusterRequestTTL = 5;  //redirect times when error or moved
    this.refresh_table_asap = false;
    this.refresh_flag = false;
    this.ready = false;
    
    this.initialize_slots_cache();
    var self = this;
    setInterval(function(){
          if(self.refresh_table_asap){
            self.refresh_table_asap = false;
            self.initialize_slots_cache();
          }
        }
       ,5000);

    return this;
}


RedisCluster.prototype.get_redis_link = function(host,port){
    var name = host+":"+port;
    if(!this.connections[name] || !this.connections[name].connected){
        if(this.connections[name]){
            this.connections[name].end();
            delete this.connections[name];
        }
        var client = redis.createClient(port,host,this.opt);
        this.connections[name]=client;
        client.on('error',function(err){
            console.error("error on client " + err);
            client.end();
        });
        if(this.role === 'slave'){
            client.send_command('READONLY',[]);
        }
    }

    return this.connections[name];
    /*
        var client = redis.createClient(port,host,this.opt);
        if(this.role === 'slave'){
                    client.send_command('READONLY',[]);
        }
        client.on('error',function(err){
            console.log("error on client " + err);
        });
        return client;
        */
}

RedisCluster.prototype.set_node_name = function(node){
    node.name = node.name || node.host+":"+node.port;
}

RedisCluster.prototype.initialize_slots_cache = function(){
    var self = this;
    this.refresh_flag = true;
    
    async.detectSeries(this.startup_nodes.sort(function() {return 0.5 - Math.random()}),function(node,cb){
        //self.slots = [];
        //self.nodes = [];
        var r = self.get_redis_link(node.host,node.port);
        r.send_command('cluster',['nodes'],function(err,reply){
           if(!err){
              self.nodes = self.parseNodes(reply.toString()); 
              self.slots = [];
              self.nodes.forEach(function(node){
                  if(node.role.match(self.role)){
                      if(!self.connections[node.address.name]){
                          //self.connections[node.address.name] = self.get_redis_link(node.address.host,node.address.port);
                          self.get_redis_link(node.address.host,node.address.port);
                      }
                      
                      var _i,_len,i,j,range;
                      for (_i = 0, _len = node.slot.length; _i < _len; _i++){
                          range = node.slot[_i].split('-');
                          i = parseInt(range[0]);
                          j = parseInt(range[1] || range[0]);
                          for(;i<=j;i++){
                              self.slots[i] = self.slots[i] || [];
                              self.slots[i].push(node.address);
                          }
                      }
                  }
              });
              //self.connections[node.host+":"+node.port] = r;
              cb(true);
           }else{
              r.quit();
              delete self.connections[node.host+":"+node.port];
              cb(false);
           }
        });
    },function(result){
        if(result){
            self.refresh_table_asap = false;
            self.populate_startup_nodes();
            self.ready = true;
            self.emit('ready');
        }
        self.refresh_flag = false;
    });
}



RedisCluster.prototype.populate_startup_nodes = function(){
    //this.startup_nodes.forEach(function(node){this.set_node_name(node);});
    this.startup_nodes = [];
    var self = this;
    this.nodes.forEach(function(node){
        self.startup_nodes.push(node.address)
    });
}

RedisCluster.prototype.get_node_by_slot = function(slot){
    var node;
    if(this.slots[slot]){
        this.slots[slot].reverse();
        node = this.slots[slot][0];
    }else{
        console.warn("slot not exist "+slot);
    }
    return node;
}

/*
RedisCluster.prototype.close_existing_connection = function(){
    while (this.connections.length >= this.max_connections){
        //this.connections
    }
}*/
RedisCluster.prototype.parseNodes = function(s){
    var address,id,data, l, lines, nodes, result, _i, _len, _ref;
    nodes = [];
    result = [];
    lines = s.split('\n');
    for (_i = 0, _len = lines.length; _i < _len; _i++) {
      l = lines[_i];
      data = l.split(" ");
      if (data[7] !== 'connected') {
        continue;
      }
      address = data[1].split(':');
      nodes[data[0]] = {
        address: {
          host:address[0],
          port:address[1],
          name: "" + address[0] + ":" + address[1]
        },
        role: data[2],
        master: data[3],
        slot: (_ref = data.slice(8)) != null ? _ref : void 0
      };
    }
    for (id in nodes) {
      if (nodes[id].role.match('slave')) {
        if(nodes[nodes[id].master]){
            nodes[id].slot = nodes[nodes[id].master].slot;
        }
      }
      delete nodes[id].master;
      result.push(nodes[id]);
    }
    return result;
}
//Return the hash slot from the key.
RedisCluster.prototype.keyslot = function(key){
    //Only hash what is inside {...} if there is such a pattern in the key.
    //Note that the specification requires the content that is between
    //the first { and the first } after the first {. If we found {} without
    //nothing in the middle, the whole key is hashed as usually.

    var s = key.indexOf('{'); 
    if(s>0){
        var e = key.indexOf('}',s+1);
        if (e && e != s+1){
            key = key.substring(s,e);
        }
    }
    return crc16(key)&(this.RedisClusterHashSlots - 1);
}

RedisCluster.prototype.get_key_from_command = function(args){
    var command = args[0].toLowerCase();
    if(command === 'info' || command === 'multi' || command === 'exec' || command === 'slaveof' || command === 'config' || command === 'shutdown'){
        return undefined;
    }else if(command === 'eval'|| command === 'evalsha'){
        return args[3].toString();
    }else{
        return args[1].toString();
    }
}

RedisCluster.prototype.get_random_connection = function(){
    var conn,
        self = this,
        nodes = this.startup_nodes.sort(function() {return 0.5 - Math.random()}); // shuffle nodes
    async.detectSeries(nodes,function(node,cb){
            self.set_node_name(node);
            conn = self.connections[node.name];
            if (!conn || !conn.connected){
                conn = self.get_redis_link(node.host,node.port,self.opt);
            }
            conn.send_command("ping",[],function(err,reply){
                if(!err && reply.toString() === "PONG"){
                    //self.connections[node.name] = conn;
                    cb(true);
                }else{
                    conn.end();
                    delete self.connections[node.name];
                    cb(false);
                }
            });
        },function(result){
            if(result){
                return conn;
            }else{
                return null;
            }
        });
    return this.connections[nodes[0].name];
}
RedisCluster.prototype.get_connection_by_slot = function(slot){
    var node = this.get_node_by_slot(slot);
    if(!node){
        return this.get_random_connection();
    }
    //set node name
    this.set_node_name(node);
    if(!this.connections[node.name] || !this.connections[node.name].connected){
        //close_existing_connection();
        //this.connections[node.name] = this.get_redis_link(node.host,node.port,this.opt);
        this.get_redis_link(node.host,node.port,this.opt);
    }

    return this.connections[node.name];
}

RedisCluster.prototype.send_cluster_command = function(args){

    var r,
        self = this,
        command = args[0],
        params = args.slice(1) || [],
        asking = false,
        cb = args[args.length - 1],
        key = this.get_key_from_command(args),
        slot = this.keyslot(key),
        try_random_node = false;
        
    if(cb && typeof cb === "function"){
        params = params.slice(0,params.length-1) || [];
    }else{
        cb=undefined;
    }

    /*
    if(this.refresh_flag){
        if(cb) cb("ERROR:init slot cache...");
        return;
    }

    if(this.refresh_table_asap){// && !this.refresh_flag){
        this.refresh_table_asap = false;
        this.initialize_slots_cache();
    }*/
    
    //push queue
    //console.log("cmd = "+ command +" params = "+params +" key= "+key+" slot ="+slot);
    async.retry(this.RedisClusterRequestTTL,function(callback,result){
        //get connection
        if(try_random_node){
            try_random_node = false;
            r = self.get_random_connection();
        }else{
            r = self.get_connection_by_slot(slot);
        }
        if(!r){
            return callback(true,"ERROR:no useable connection to slot "+slot);
        }
        //asking if need
        if(asking){
            r.send_command('asking',[]);
        }
        //send command 
        //r.send_command(command,params,function(err,reply){
        r[command](params,function(err,reply){
            if(err){
                var errMsg = err.toString();
                console.error("error on "+command+":"+errMsg);
                if(errMsg.match("MOVED") || errMsg.match("ASK")){
                    if(errMsg.match("ASK")){
                        asking = true;
                    }else{
                        if(!self.refresh_flag){
                            self.refresh_table_asap = true;
                        }
                    }
                    var msg = err.toString().split(" "),
                        newslot = msg[2],
                        address = msg[3].split(":"),
                        node_ip   = address[0],
                        node_port = address[1];
                        self.slots[newslot]= [];
                        self.slots[newslot].push({host:node_ip,port:node_port,name:node_ip+":"+node_port});
                        //console.log(JSON.stringify(params)+" slotssss "+newslot + ": "+JSON.stringify(self.slots[newslot]));
                    err = "Error: Redirect";
                }else if(errMsg.match("connection|CLUSTERDOWN|ECONNREFUSED|ETIMEDOUT|CannotConnectError|EACCES")){
                    try_random_node = true;
                }
            }
            callback(err,reply);
        });
      },function(err,result){
         if(cb) cb(err,result);
    });
}

commands.forEach(function(command) {
    return RedisCluster.prototype[command.toUpperCase()] = RedisCluster.prototype[command] = function() {
        var args = Array.prototype.slice.call(arguments) || [];
        args.unshift(command);
        return this.send_cluster_command(args);
    };
});

RedisCluster.prototype.script = function(){
        var args = Array.prototype.slice.call(arguments) || [];
}

function createClient(startup_nodes,opt){
    var rcluster = RedisCluster(startup_nodes);
    return rcluster.createClient(opt);
}

//module.exports.RedisCluster = RedisCluster;
module.exports.createClient = createClient;
