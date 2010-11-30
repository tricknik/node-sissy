var Sissy = require('sissy').Sissy,
  fs = require('fs');

var config = require('test_config');

var S3 = new Sissy(config.access_key, config.secret_key, {storage_type : 'REDUCED_REDUNDANCY'});
var bucket = S3.bucket(config.host, config.bucket);
bucket.on('error', function(err) {
  console.log(err);
  console.log(err.stack);
});
bucket.upload_file('the_art_of_war.txt', 'uploaded.txt');
setTimeout(function() {
  bucket.download_file('uploaded.txt', 'downloaded.txt');
}, 2000);
