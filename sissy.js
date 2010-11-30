/***************************************************
 *
 * Sissy: Yet Another S3 Module for Node 
 * Dmytri Kleiner <dk@trick.ca>
 *
 * This program is free software. 
 * It comes without any warranty, to the extent permitted by
 * applicable law. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License v2.
 * See http://sam.zoy.org/wtfpl/COPYING for more details. 
 *
 * This work was originaly commissiond by SoundCloud
 *
 * What is SoundCloud?
 * http://soundcloud.com/tour
 *
 ***********************************/

var events = require('events'),
  fs = require('fs'),
  path = require('path'),
  sys = require('sys'),
  dns = require('dns'),
  net = require('net'),
  http = require('http'),
  crypto = require('crypto'),
  mime = require('node-mime/mime');

/* Bucky is an S3 Bucket */
var Bucky = function(account, host, bucke, options){
  options = options || function(){};
  this.account = account;
  this.host = host;
  this.bucket = bucket;
  this.storage_type = options.storage_type || 'STANDARD';  // standard storage type
  this.acl = options.acl || 'private'; // set status to private on upload
  this.check_md5 = options.check_md5 || true;  // check the md5 on upload
};
sys.inherits(Bucky, events.EventEmitter);

Bucky.prototype.authorize = function(method, target, headers, amz_headers) {
  vart buckm = this,
    content_type = headers['Content-Type'] || '',
    md5 =  headers['Content-MD5'] || '',
    current_date = headers.Date || new Date().toUTCString(),
    amz_headers_list = [];
  if (amz_headers) {
    for (var header in amz_headers){
      var key = header.toString().toLowerCase(),
        value = amz_headers[header];
      if (key == 'x-amz-date') {
        current_date = '';
      } else if(value instanceof Array) {
        value = value.join(',');
      }
      headers[key] = value;
      amz_headers_list.push(key + ':' + value);
    }
    var amz_headers_string = amz_headers_list.sort().join('\n');
  }
  var authorization_string = method + "\n" + md5 + "\n" + content_type + "\n" + current_date + "\n";
  if (amz_headers) {
    authorization_string += amz_headers_string + "\n";
  }
  authorization_string +=  '/' + bucky.bucket + '/' + target;
  hmac = crypto.createHmac('sha1', bucky.account.secret_key),
  hmac.update(authorization_string);
  headers.Authorization = 'AWS ' + bucky.account.access_key + ":" + hmac.digest(encoding = 'base64');
};

Bucky.prototype.upload_file = function(file, target){
  var bucky = this;
  target = target || path.basename(file);
  fs.stat(file, function(err, stats){
    if (err) {
      bucky.emit('error', err);
    } else {
      var send = function(md5) {
        var read_stream = fs.createReadStream(file);
        bucky.upload(read_stream, target, stats.size, md5)
      };
      if(bucky.check_md5 === true){
        var hash = crypto.createHash('md5');
        var file_stream = fs.createReadStream(file);
        file_stream.on('data', function(data){
          hash.update(data);
        });
        file_stream.on('error', function(err){
          bucky.emit('error', err);
        });
        file_stream.on('end', function(){
          send(hash.digest(encoding = 'base64'));
        });
      } else {
        send();
      }
    }
  });
};

Bucky.prototype.upload = function(read_stream, target, content_length, md5){
  read_stream.pause();
  var bucky = this;
  dns.resolve4(bucky.host, function(err, addresses) {
    if (err) {
        bucky.emit('error', err);
    } else {
      net_stream = net.createConnection(80, host=addresses[0]);
      net_stream.on('error', function(err){
        bucky.emit('error', err);
      });
      read_stream.on('error', function(err){
        bucky.emit('error', err);
      });
      read_stream.on('end', function() {
        net_stream.end();
      });
      bucky.put(read_stream, net_stream, target, content_length, md5);
    }
  });
};

Bucky.prototype.streaming = false;
Bucky.prototype.put = function(read_stream, net_stream, target, content_length, md5) {
  var bucky = this,
    mime_type = mime.lookup(target),
    headers = {
    'Date': new Date().toUTCString(),
    'Host': bucky.host,
    'Content-Type': mime.lookup(target),
    'Expect': '100-continue',
  };
  if (content_length) {
    headers['Content-Length'] = content_length;
  }
  if (md5) {
    headers['Content-MD5'] = md5;
  }
  amz_headers = {
    'x-amz-storage-class': bucky.storage_type,
    'x-amz-acl': bucky.acl
  };
  bucky.authorize('PUT', target, headers, amz_headers);
  net_stream.on('data', function (data) { 
    if (!bucky.streaming) {
      bucky.streaming = true;
      var continue_header = /100\s+continue/i;
      if (continue_header.test(data)) {
        read_stream.resume();
      } else {
        read_stream.destroy();
        net_stream.end();
        err = Error(data);
        bucky.emit('error', err);
      }
    }
  });
  net_stream.on('connect', function() {
    var header_string = "PUT " + '/' + target + " HTTP/1.1" + "\n"
    for(var header in headers) {
      if (headers[header] !== '') {
        header_string += header + ': ' + headers[header] + "\r\n";
      }
    }
    net_stream.write(header_string += "\r\n");
    sys.pump(read_stream, net_stream, function(err) {
      if (err) {
        read_stream.end();
        bucky.emit('error', err);
      }
    });
  });
};

Bucky.prototype.download_file = function(file, target, range){
  var bucky = this;
  target = target || path.basename(file);
  var write_stream = fs.createWriteStream(target);
  write_stream.on('open', function(fd) {
    bucky.get(write_stream, file, range);
  });
  write_stream.on('close', function() {
    fs.stat(file, function(err, stat) {
      if (!err && stat.size == 0) {
        fs.unlink(file);
      }
    });
  });
  write_stream.on('error', function(ex){
    write_stream.destroy();
    bucky.emit('error', ex);
  });
};

Bucky.prototype.get = function(write_stream, file, range){
  var bucky = this, expected = '200';
    http_client = http.createClient(80, bucky.host);
  var headers = {
    'Date': new Date().toUTCString(),
    'Host': bucky.host,
  };
  if (range) {
    expected = '206';
    headers['Range'] = range;
  }
  bucky.authorize('GET', file, headers);
  var request = http_client.request('GET', '/' + file, headers);
  request.end();
  request.on('response', function (response) {
    if (response.statusCode == expected) {
      response.on('end', function(){
        write_stream.end();
        bucky.emit('complete');
      });
      sys.pump(response, write_stream);
    } else {
      write_stream.end();
      err = Error(response.statusCode + JSON.stringify(response.headers));
      bucky.emit('error', err);
    }
  });
};

/* Sissy is an S3 Account */
var Sissy = function(access_key, secret_key) {
  this.access_key = access_key;
  this.secret_key = secret_key;
};

Sissy.prototype.bucket = function(host, bucket, options) {
  return new Bucky(this, host, bucket, options);
}

exports.Sissy = Sissy;

