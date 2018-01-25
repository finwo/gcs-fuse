var fuse      = require('fuse-bindings'),
    stat      = require('./fixtures/stat'),
    Storage   = require('@google-cloud/storage');

var storage = false,
    bucket  = false,
    path    = '';

require('yargs')
  .demand(1)
  .command('daemon <fname> <bucket> <path>', 'start the server', (yargs) => {
    yargs
      .positional('fname', {
        describe: 'Authentication key file'
      })
      .positional('bucket', {
        describe: 'Name of the bucket to mount'
      })
      .positional('path', {
        describe: 'Path to mount the bucket on'
      })
  }, (argv) => {

    // Setup & connect
    storage = new Storage({ keyFilename: argv.fname });
    bucket  = storage.bucket(argv.bucket);
    path    = argv.path;

    fuse.mount( path, {
      options: argv.options.split(','),

      init: function(cb){
        console.log('INIT!')
        cb(0);
      },
      access: function(path, mode, cb) {
        console.log('ACCESS: ' + mode + ' ' + path);
        cb(0);
      },
      statfs: function(path, cb) {
        console.log('STATFS: ' + path);
        cb(0, {
          bsize: 1000000,
          frsize: 1000000,
          blocks: 1000000,
          bfree: 1000000,
          bavail: 1000000,
          files: 1000000,
          ffree: 1000000,
          favail: 1000000,
          fsid: 1000000,
          flag: 1000000,
          namemax: 1000000
        });
      },
      getattr: function(path, cb){
        console.log('GETATTR: ' + path);
        cb(0, {
          mtime: new Date(),
          atime: new Date(),
          ctime: new Date(),
          size: 100,
          mode: 16877,
          uid: process.getuid(),
          gid: process.getgid()
        });
      },
      fgetattr: function(path, fd, cb){cb();},
      flush: function(path, fd, cb){cb();},
      fsync: function(path, fd, datasync, cb){cb();},
      fsyncdir: function(path, fd, datasync, cb){cb();},
      readdir: function f_readdir(path, cb, tries) {
        tries = tries || 0;
        console.log('READDIR: ' + path + ' (' + tries + ')');
        path = path.substr(1);
        bucket
          .getFiles()
          .then(function(results) {
            console.log('  Success');
            var files = results.shift();
            cb(0, files
              .map(function(fileObject) {
                console.log('  Map fileObject to name');
                return fileObject.name;
              })
              .filter(function(fullname) {
                console.log('  Filter by prefix');
                return !path.length || fullname.substr(path.length) === path;
              })
              .map(function(fullname) {
                console.log('  Map to basename list');
                return fullname.substr(path.length).split('/',2);
              })
              .filter(function(basenameList) {
                console.log('  Filter by recursion');
                return basenameList.length === 1 || basenameList[1].length === 0;
              })
              .map(function(basenameList) {
                console.log('  Map to string: ' + basenameList[0])
                return basenameList.shift();
              })
            );
          })
          .catch(function (err) {
            if ( tries > 10 ) throw err;
            f_readdir( path, cb, tries + 1 );
          })
      },
      truncate: function(path, size, cb){cb();},
      ftruncate: function(path, fd, size, cb){cb();},
      readlink: function(path, cb){cb();},
      chown: function(path, uid, gid, cb){cb();},
      chmod: function(path, mode, cb){cb();},
      mknod: function(path, mode, dev, cb){cb();},
      setxattr: function(path, name, buffer, length, offset, flags, cb){cb();},
      getxattr: function(path, name, buffer, length, offset, cb){cb();},
      listxattr: function(path, buffer, length, cb){cb();},
      removexattr: function(path, name, cb){cb();},
      open: function(path, flags, cb){cb();},
      opendir: function(path, flags, cb){cb();},
      read: function(path, fd, buffer, length, position, cb){cb();},
      write: function(path, fd, buffer, length, position, cb){cb();},
      release: function(path, fd, cb){cb();},
      releasedir: function(path, fd, cb){cb();},
      create: function(path, mode, cb){cb();},
      utimens: function(path, atime, mtime, cb){cb();},
      unlink: function(path, cb){cb();},
      rename: function(src, dest, cb){cb();},
      link: function(src, dest, cb){cb();},
      symlink: function(src, dest, cb){cb();},
      mkdir: function(path, mode, cb){cb();},
      rmdir: function(path, cb){cb();},
      destroy: function(cb){cb();}
    }, function(err) {
      if(err) throw err;
      console.log( bucket.id + ' mounted on ' + path );
    })
  })
  .option('verbose', {
    alias: 'v',
    default: false
  })
  .option('options', {
    alias: 'o',
    default: ''
  })
  .argv

process.on('SIGINT', function() {
  fuse.unmount(path,function(err) {
    if(err) {
      console.error('Couldn\'t unmount ' + path);
    } else {
      console.log( path + ' unmounted');
    }
  })
});

// function updateDelimiter() {
//   if( !storage   ) projectId  = false;
//   if( !projectId ) bucketList = [];
//   if( !projectId ) bucket     = false;

//   cli.delimiter(
//     ( ( storage && projectId ) ? '[ ' + projectId : '' ) +
//     ( ( storage && bucket    ) ? ':'  + bucket.id : '' ) +
//     ( ( storage && projectId ) ? ' ] '            : '' ) +
//     '>'
//   );
// }

// cli
//   .command('connect [keyFilename]', 'Connect to a google cloud project')
//   .action(function(args, cb) {
//     if(storage) {
//       return cb('Error: already connected');
//     }
//     var fname = args.keyFilename + '.json';
//     if (fname.substr(0,1) !== '/') fname = './' + fname;
//     storage = new Storage({
//       keyFilename: fname
//     });
//     projectId = require(fname).project_id;
//     updateDelimiter();
//     cb();
//   });

// cli
//   .command('disconnect', 'Disconnect from a google cloud project')
//   .action(function(args, cb) {
//     if(!storage) {
//       return cb('Error: not connected');
//     }
//     storage = false;
//     updateDelimiter();
//     cb();
//   })

// cli
//   .command('list-buckets', 'List all the buckets in the connected project')
//   .action(function(args, cb) {
//     if(!storage) {
//       return cb('Error: not connected');
//     }
//     storage
//       .getBuckets()
//       .then(function(bucketListList) {
//         bucketList = bucketListList.shift();
//         cb(bucketList.map((b)=>b.id));
//       });
//   });

// cli
//   .command('open-bucket <id>', 'Open a bucket')
//   .autocompletion(function openBucketCompletion (text, iteration, cb) {
//     if(!storage) {
//       return cb('Error: not connected');
//     }
//     if(!bucketList.length) {
//       var self = this;
//       cli.exec('list-buckets')
//         .then(function() {
//           openBucketCompletion.call(self,text,iteration,cb);
//         })
//         .catch(function() {
//           openBucketCompletion.call(self,text,iteration,cb);
//         })
//     } else {
//       cb(void 0,bucketList.map((b)=>b.id));
//     }
//   })
//   .action(function(args, cb) {
//     bucket = storage.bucket(args.id);
//     updateDelimiter();
//     cb();
//   })

// cli
//   .command('ls', 'List all files')
//   .action(function bucketLs(args, cb) {
//     if(!bucket) {
//       return cb('Error: No bucket is currently opened')
//     }
//     var self = this;
//     bucket
//       .getFiles()
//       .then(function(results) {
//         results = results.shift();
//         cb(results.map((f)=>f.name));
//       })
//       .catch(function() {
//         bucketLs.call(self,args,cb);
//       })
//   })

// cli
//   .command('clear', 'Clear the contents of the console')
//   .action(function(args, callback) {
//     cli_clear();
//     callback();
//   });

// cli
//   .delimiter('>')
//   .show();
