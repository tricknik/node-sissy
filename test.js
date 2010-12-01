var Sissy = require('sissy').Sissy,
  fs = require('fs');

var config = require('test_config');

var S3 = new Sissy(config.access_key, config.secret_key, {storage_type : 'REDUCED_REDUNDANCY'});
var up = S3.bucket(config.host, config.bucket);
up.on('error', function(err) {
  console.log(err);
  console.log(err.stack);
});
console.log('Uploading....');
up.upload_file('the_art_of_war.txt', 'uploaded.txt');
up.on('complete', function() {
  console.log('Upload Complete');
  setTimeout(function() {
    console.log('Downloading....');
    down = S3.bucket(config.host, config.bucket);
    down.on('error', function(err) {
      console.log(err);
      console.log(err.stack);
    });
    down.download_file('uploaded.txt', 'downloaded.txt');
    down.on('complete', function() {
      console.log('Download Complete');
    });
  }, 2000);
});
