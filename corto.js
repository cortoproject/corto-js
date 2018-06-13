
if (!corto) {
   var corto = {};
   corto.subscribers = {};
   corto.metadata = {};
   corto.retryConnection = undefined;
}

const CORTO_PRIMITIVE = 0;
const CORTO_COMPOSITE = 1;
const CORTO_COLLECTION = 2;
corto.retryPeriod = 100;
corto.retries = 0;

// Message classes
corto.msg_connect = function() {
  this.type = "ws/connect";
  this.value = {};
  this.value.version = "1.0";
}

corto.msg_subscribe = function(id, parent, expr, type, offset, limit, summary, yield_unknown) {
  this.type = "ws/sub";
  this.value = {};
  this.value.id = id;
  this.value.parent = parent;
  this.value.expr = expr;
  this.value.type = type;
  this.value.offset = offset;
  this.value.limit = limit;
  this.value.summary = summary;
  this.value.yield_unknown = yield_unknown;
}

corto.msg_unsubscribe = function(id) {
  this.type = "ws/unsub";
  this.value = {};
  this.value.id = id;
}

corto.msg_update = function(id, value) {
  this.type = "ws/update";
  this.value = {};
  this.value.id = id;
  this.value.v = value;
}

corto.msg_delete = function(id) {
  this.type = "ws/delete";
  this.value = {};
  this.value.id = id;
}

corto.unpack = function(value, type) {
  var result;

  if (type.kind == CORTO_PRIMITIVE) {
    result = value;
  } else 
  if (type.kind == CORTO_COMPOSITE) {
    result = {};
    var count = 0;
    type.members.forEach(member => {
      var member_type = corto.metadata[member[1]];
      result[member[0]] = corto.unpack(value[count ++], member_type);
    });
  } else
  if (type.kind == CORTO_COLLECTION) {
    result = value;
  }

  return result;
}

// Subscriber class
corto.subscriber = function(id, db, from, select, type, offset, limit, on_update, on_delete, on_ok, on_error) {
  this.id = id;
  this.select = select;
  this.from = from;
  this.type = type;
  this.offset = offset;
  this.limit = limit
  this.db = db;
  this.meta_db = [];
  this.enabled = false;
  this.on_update = on_update;
  this.on_delete = on_delete;
  this.on_ok = on_ok;
  this.on_error = on_error;

  this.findType = function(id) {
    return this.db.find(function(elem) {
      return elem.id == id;
    });
  }

  this.insert_object = function(object, type) {
    var cached_object = this.db.find(el => el.id == object.id);
    if (!cached_object) {
      cached_object = {id: object.id, type: type, value: {}}
      this.db.push(cached_object);
    }

    var v = corto.unpack(object.v, type);
    cached_object.value = v;
  }

  this.delete_object = function(id) {
    var index = this.db.findIndex(el => el.id == id);
    this.db.splice(index, 1);
  }
}

corto.connected = function(msg) {
  if (corto.on_connected != undefined) corto.on_connected(msg);
}

corto.insert_type = function(msg) {
  var is_composite = [
    "struct", "class", "container", "table", "leaf", "procedure", "union"]
      .includes(msg.kind);

  var is_collection = [
    "array", "sequence", "list", "map"].includes(msg.kind);

  return corto.metadata[msg.type] = {
    id: msg.type,
    kind: is_composite 
        ? CORTO_COMPOSITE 
        : is_collection 
            ? CORTO_COLLECTION
            : CORTO_PRIMITIVE,
    members: msg.members,
    constants: msg.constants,
    reference: msg.reference
  }
}

corto.insert = function(msg) {
  var subscriber = corto.subscribers[msg.sub];

  // Walk through types in message
  msg.data.forEach(el => {
    // If 'kind' is set in the message, it contains metadata. Insert
    // type in the metadata repository.
    var type;
    if (el.kind) {
      type = corto.insert_type(el);
    } else {
      type = corto.metadata[el.type];
    }

    // Walk through object updates
    if (type) {
      if (el.set) {
        el.set.forEach(object => {
          subscriber.insert_object(object, type);
        }); 
      }

      // Walk through object deletes
      if (el.del) {
        el.del.forEach(object => {
          console.log(object.id);
          subscriber.delete_object(object.id);
        });
      }
    } else if (el.set || el.del) {
      console.log(msg);
      console.error(
        "received deletes/updates for unknown type " + el.type);
    }
  });
}

corto.subok = function(msg) {
  if (msg.id in corto.subscribers) {
    subscriber = corto.subscribers[msg.id];
    subscriber.enabled = true;
    if (subscriber.on_ok) {
        subscriber.on_ok();
    }
  }
}

corto.subfail = function(msg) {
  if (msg.id in corto.subscribers) {
    subscriber = corto.subscribers[msg.id];
    if (subscriber.on_error) {
      if (subscriber.instance) {
        subscriber.on_error(subscriber.instance, msg.error);
      } else {
        subscriber.on_error(msg.error);
      }
    }
  }
  delete corto.subscribers[msg.id];
}

corto.recv_handlers = {
  "/corto/ws/connected": function(msg) { corto.connected(msg.value) },
  "/corto/ws/data": function(msg) { corto.insert(msg.value) },
  "/corto/ws/subok": function(msg) { corto.subok(msg.value) },
  "/corto/ws/subfail": function(msg) { corto.subfail(msg.value) }
}

corto.recv = function(msg) {
  corto.recv_handlers[msg.type](msg);
}

corto.send = function(msg) {
  this.ws.send(JSON.stringify(msg));
}

corto.connectToWs = function(host, on_open, on_error, on_close) {
  corto.ws = new WebSocket("ws://" + host);

  corto.ws.onopen = function(ev) {
    corto.retryConnection = undefined;
    if (on_open != undefined) on_open();
    corto.send(new corto.msg_connect());
  };
  corto.ws.onerror = function(ev) {
    if (on_error != undefined) on_error();
  };
  corto.ws.onclose = function(ev) {
    if (corto.host == host) {
      if (!corto.retryConnection) {
        if (on_close != undefined) on_close();
      }
      if (corto.retries < 10) {
        corto.retries ++;
      }
      corto.retryConnection = setTimeout(function() {
        corto.connectToWs(host, on_open, on_error, on_close);
      }, corto.retryPeriod * corto.retries);
    }
  };
  corto.ws.onmessage = function(ev) {
    var msg = JSON.parse(ev.data);
    if (msg) {
      corto.recv(msg);
    }
  }
}

corto.connect = function(params) {
  var host = params.host;
  var on_connected = params.on_connected;
  var on_open = params.on_open;
  var on_error = params.on_error;
  var on_close = params.on_close;

  corto.retries = 0;

  if (corto.retryConnection) {
    clearTimeout(corto.retryConnection);
  }

  if (corto.ws && corto.ws.readyState != 3 && corto.host != host) {
    var ws = corto.ws;
    corto.ws = undefined;
    ws.close();
  }

  corto.retryConnection = undefined;

  this.host = host;
  this.on_connected = on_connected;
  corto.connectToWs(host, on_open, on_error, on_close);
}

corto.subscribe = function({
  id, select, from, type, offset, limit, db, on_define, on_update, on_delete, on_ok, on_error, summary}) {
  db.splice(0);
  corto.subscribers[id] =
    new corto.subscriber(
      id, db, from, select, type, offset, limit, on_update, on_delete, on_ok, on_error);
  corto.send(new corto.msg_subscribe(id, from, select, type, offset, limit, summary, true));
}

corto.unsubscribe = function(params) {
  var id = params.id;
  if (corto.subscribers[id]) {
    corto.send(new corto.msg_unsubscribe(id));
    corto.subscribers[id].enabled = false;
    delete corto.subscribers[id];
  }
}

corto.lookup = function(db, id) {
  var result = db.find(function(elem) {
    return elem.id == id;
  });
  if (result) return result;
}

corto.lookup_table = function(db, id) {
  return db.find(function(elem) {
    return elem.id == id;
  });
}

corto.publish = function(params) {
  var id = params.id;
  var value = params.value;
  corto.send(new corto.msg_update(id, value));
}

corto.delete = function(params) {
  var id = params.id;
  corto.send(new corto.msg_delete(id));
}

corto.parseQuery = function(query) {
  var re = /select ([\*\.a-zA-Z0-9_\~\^\|\&\-\/\\]+) *(from ([\*\.a-zA-Z0-9_\~\-\/\\]+))? *(type ([\*\.a-zA-Z0-9_\^\|\&\~\-\/\\]+))?/;
  var matches = query.match(re);
  if (matches) {
    return {
      select: matches[1],
      from: matches[3],
      type: matches[5]
    };
  } else {
    return {}
  }
}
