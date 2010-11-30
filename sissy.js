var EventEmitter = require('events').EventEmitter,
  sys = require('sys'),
  http = require('http'),
  crypto = require('crypto'),
  fs = require('fs'),
  path = require('path'),
  net = require('net'),
  dns = require('dns'),
  mime = require('node-mime/mime');


var Bucky = function(account, host, bucket, options){
  options = options || function(){};
  this.account = account;
  this.host = host;
  this.bucket = bucket;
  this.storage_type = options.storage_type || 'STANDARD';  // standard storage type
  this.acl = options.acl || 'private'; // set status to private on upload
  this.check_md5 = options.check_md5 || true;  // check the md5 on upload
};
sys.inherits(Bucky, events.EventEmitter);

Bucky.prototype.authorize = function(headers, amz_headers, method, target){
  var bucky = this,
    hmac = crypto.createHmac('sha1', bucky.account.secret_key),
    content_type = headers['Content-Type'] || '',
    md5 =  headers['Content-MD5'] || '',
    current_date = headers.Date || new Date().toUTCString(),
    amz_headers_list;
  for (var header in amz_headers){
    var key = header.toString().toLowerCase(),
      value = headers[header];
      if (key == 'x-amz-date') {
        current_date = '';
      } else if(value instanceof Array) {
          value = value.join(',');
      }
      headers[key] = value;
      amz_headers_list.push(key + ':' + value);
    }
  }
  var amz_headers_string = amz_headers_list.sort().join('\n');
  var authorization_string =
    method + "\n" +
    md5 + "\n" + // (optional)
    content_type + "\n" + // (optional)
    current_date + "\n" + // only include if no x-amz-date
    amz_headers_string + "\n"// can be blank
    '/' + bucky.bucket + '/' + target;
  hmac.update(authorization_string);
  headers.Authorization = 'AWS ' + bucky.account.access_key + ":" + hmac.digest(encoding = 'base64');
};

Bucky.prototype.upload_file = function(file, target){
  var bucky = this,
    secret_key = this.secret_key,
    access_key = this.access_key;
  target = target || path.basename(file);
  fs.stat(file, function(err, stats){
    if (err) {
      bucky.emit('error', err);
    } else {
      var send = function(md5) {
        var read_stream = fs.createReadFileStream(file);
        bucky.upload(read_stream, target, stats.size, md5)
      };
      if(bucky.check_md5 === true){
        var hash = crypto.createHash('md5');
        var file_stream = fs.createReadFileStream(file);
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
      bucky.put(read_stream, net_stream, target, stats.size, md5);
    }
  });
};

Bucky.prototype.put = function(read_stream, net_stream, target, content_length, md5){
  var bucky = this,
    mime_type = mime.lookup(target),
  var headers = {
    'Date': new Date().toUTCString(),
    'Host': host,
    'Content-Type': mimeType,
    'Expect': '100-continue',
  };
  if (content_length) {
    headers['Content-Length'] = content_length,
  }
  if (md5) {
    headers['Content-MD5'] = md5;
  }
  amz_headers = {
    'x-amz-storage-class': bucky.storage_type,
    'x-amz-acl': bucky.acl
  };
  bucky.authorize(headers, amz_headers, 'PUT', target);
  net_stream.on('data', function (data) { 
    var continue_header = /100\s+continue/i;
    var error_header = /400\s+Bad\s+Request/i;
    if(continue_header.test(data)){
      read_stream.resume();
    }
    if(error_header.test(data)){
      err = Error(data);
      read_stream.end();
      net_stream.end();
      bucky.emit('error', err);
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
});

var Sissy = function(secret_key, access_key){
  this.secret_key = secret_key;
  this.access_key = access_key;
};

Sissy.prototype.bucky = function(host, bucket){
  return new Bucky(this, host, bucket, options);
}

exports.Sissy = Sissy;

