var fuse           = require('fuse-bindings'),
    stat           = require('./fixtures/stat'),
    Storage        = require('@google-cloud/storage'),
    streamToBuffer = require('stream-to-buffer'),
    Promise        = require('bluebird');

var storage         = false,
    bucket          = false,
    path            = '',
    fileDescriptors = [];

function flagDecode( flags ) {
  switch( flags & 3 ) {
    // case 0b00000000000000001000000000000000: return 'r';
    // case 0b00000000000000001000000000000010: return 'r+';
    // case 0b00000000000000001001000000000000: return 'rs';
    // case 0b00000000000000001001000000000010: return 'rs+';
    // case 0b00000000000000001000000000000001: return 'w';
    // case 0b00000000000000001000000000000010: return 'w+';
    // case 0b00000000000000001000010000000001: return 'a';
    // case 0b00000000000000001000010000000010: return 'a+';
    case 0: return 'r';
    case 1: return 'w';
    default: return 'r+';
  }
}

function f_bucket_getfiles() {
  return new Promise(function(resolve, reject) {
    var tstamp = new Date().getTime(),
        key    = 'f_bucket_getfiles';
    if ( cache[key] && ( cache[key].expires > tstamp ) ) return resolve(cache[key].data);
    bucket.getFiles()
      .then(function(result) {
        result = result.shift();
        cache[key] = { expires: (new Date().getTime()) + 100,
                       data   : result };
        resolve(result);
      })
      .catch(reject);
  });
}

var cache = {},
    cacheCleanTO = setTimeout(function f_clean_cache() {
      var tstamp      = new Date().getTime(),
          nextExpires = 0;
      Object.keys(cache).forEach(function(key) {
        if ( cache[key].expires <= tstamp ) {
          delete cache[key];
        } else {
          nextExpires = Math.min( nextExpires, cache[key].expires ) || cache[key].expires;
        }
      });
      nextExpires = nextExpires || ( tstamp + 10000 );
      cacheCleanTO = setTimeout(f_clean_cache,nextExpires-tstamp);
    }, 0);

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
    var ops;

    fuse.mount( path, ops = {
      options: argv.options.split(','),
      force  : true,

      getattr: function f_getattr( path, cb, objMode, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        console.log('GETATTR',tries,path);
        if ( path === '/' ) return cb( null, stat({ mode: 'dir', size: 4096 }));
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        var dirname = path.split('/');
        dirname.pop();
        dirname = '/' + dirname.join('/');
        ops.readdir( dirname, function( err, dirEntries ) {
          var fileObject = dirEntries
            .filter(function(dirEntry) {
              if ('string' === typeof dirEntry) return false;
              var name = dirEntry.name;
              if ( name.slice(-1) === '/' ) name = name.slice(0,-1);
              return name === path;
            })
            .shift();
          if (!fileObject) return cb(fuse.ENOENT);
          var attr = { mode : 'file',
                       size : parseInt(fileObject.metadata.size),
                       mtime: Date.parse( fileObject.metadata.updated ),
                       ctime: Date.parse( fileObject.metadata.updated ) };
          if ( fileObject.name.slice(-1) === '/' ) {
            attr.mode = 'dir';
            attr.size = 4096;
          }
          if ( objMode ) {
            attr.fileObject = fileObject;
            return cb( null, attr );
          }
          cb(null,stat(attr));
        }, true, 0);
      },

      readdir: function f_readdir( path, cb, objMode, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        console.log('READDIR',tries,objMode,path);
        if ( path.slice(-1) != '/' ) path += '/';
        while ( path.substr(0,1) === '/' ) path = path.substr(1);

        var cacheKey = 'f_readdir_' + (objMode?'1':'0') + '_' + path;
        if( cache[cacheKey] && cache[cacheKey].expires > (new Date().getTime()) ) {
          return cb( cache[cacheKey].code, cache[cacheKey].data );
        }

        f_bucket_getfiles()
          .then(function(results) {
            var list = results
              .filter(function(fileObject) {
                if ( fileObject.name.substr(0,path.length) !== path ) return false;
                if ( fileObject.name.substr(path.length).split('/').filter((n)=>n).length > 1 ) return false;
                return true;
              })
              .map(function(fileObject) {
                if ( objMode ) return fileObject;
                if ( fileObject.name === path ) return false;
                var basename = fileObject.name.substr(path.length);
                if ( basename.slice(-1) === '/' ) basename = basename.slice(0,-1);
                return basename;
              })
              .filter(function(basename) {
                return !!basename;
              })
            cache[cacheKey] = {
              expires: (new Date().getTime()) + 100,
              code   : null,
              data   : ['.','..'].concat(list),
            }
            cb(null,['.','..'].concat(list));
          })
          .catch(function(err) {
            console.log(err);
            f_readdir( path, cb, objMode, tries + 1 );
          })
      },

      open: function f_open( path, flags, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        flags = ( 'string' === typeof flags ) ? flags : flagDecode( flags );
        console.log('OPEN',tries,path,flags);

        ops.getattr( path, function( err, attr ) {
          if ( err ) {
            switch(err) {
              case fuse.ENOENT:
                return cb(err);
              case fuse.EIO:
                return f_open(path,flags,cb,tries+1);
              default:
                return f_open(path,flags,cb,tries+1);
            }
          }
          if ( attr.mode === 'dir' ) return cb(fuse.EISDIR);
          switch(flags) {
            case 'r':
              var fd = { id: 5, mode: flags, attr: attr, fo: attr.fileObject };
              fileDescriptors.forEach(function(lfd) {
                fd.id = Math.max( fd.id, lfd.id + 1 );
              });
              fileDescriptors.push(fd);
              return cb(0,fd.id);
            default:
              return cb(fuse.EBADF);
          }
        }, true, 0 );
      },

      release: function f_release( path, fd, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        console.log('RELEASE',tries,fd,path);
        fileDescriptors = fileDescriptors.filter(function(lfd) {
          return lfd.id != fd;
        });
        cb(0);
      },

      read: function f_read( path, fd, buf, len, pos, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        console.log('READ',tries,fd,pos,len,path);
        var lfd = fileDescriptors.filter(function(lfdo) {
          return lfdo.id == fd;
        }).shift();
        if(!lfd)return cb(fuse.ENOENT);

        streamToBuffer(
          lfd.fo.createReadStream({ start: pos, end: Math.min( pos+len, parseInt(lfd.fo.metadata.size) - pos ) }),
          function( err, buffer ) {
            if(err)return f_read(path,fd,buf,len,pos,cb,tries+1);
            buf.write(buffer.toString());
            cb(buffer.length);
          }
        );
      },

      // fgetattr: function(path, fd, cb) {
      //   console.log('FGETATTR', path, fd )
      //   cb();
      // },
      // flush: function(path, fd, cb){cb();},
      // fsync: function(path, fd, datasync, cb){cb();},
      // fsyncdir: function(path, fd, datasync, cb){cb();},
      // truncate: function(path, size, cb) {
      //   console.log('TRUNCATE',path,size);
      //   cb();
      // },
      // ftruncate: function(path, fd, size, cb) {
      //   console.log('FTRUNCATE',path,fd,size)
      //   cb();
      // },
      // readlink: function(path, cb) {
      //   console.log('READLINK',path)
      //   cb();
      // },
      // chown: function(path, uid, gid, cb) {
      //   console.log('CHOWN',path,uid,gid);
      //   cb(0);
      // },
      // chmod: function(path, mode, cb) {
      //   console.log('CHMOD:',path,mode);
      //   cb(0);
      // },
      // mknod: function(path, mode, dev, cb) {
      //   console.log('MKNOD', path, mode, dev);
      //   cb();
      // },
      // setxattr: function(path, name, buffer, length, offset, flags, cb) {
      //   console.log('SETXATTR',path,name,length,offset,flags);
      //   cb();
      // },
      // getxattr: function(path, name, buffer, length, offset, cb) {
      //   console.log('GETXATTR',path,name,length,offset);
      //   cb();
      // },
      // listxattr: function(path, buffer, length, cb) {
      //   console.log('LISTXATTR',path,length);
      //   cb();
      // },
      // removexattr: function(path, name, cb) {
      //   console.log('REMOVEXATTR',path,name);
      //   cb();
      // },
      // open: function(path, flags, cb) {
      //   console.log('OPEN',path,flags);
      //   var fl = flagDecode(flags);
      //   switch(fl) {
      //     case 'r':
      //       mntOpts.getattr( path, function( err, stat, fileObject ) {
      //         var fd = { id: 5, fo: fileObject };
      //         fileDescriptors.forEach(function(tmpfd) {
      //           fd.id = Math.max( fd.id, tmpfd.id + 1 );
      //         });
      //         fileDescriptors.push(fd);
      //         console.log()
      //         cb(0,fd.id);
      //       });
      //       break;
      //     default:
      //       return cb();
      //   }
      // },
      // opendir: function(path, flags, cb) {
      //   console.log('OPENDIR',path,flags);
      //   cb();
      // },
      // read: function(path, fd, buffer, length, position, cb){
      //   console.log( 'READ', path, fd, buffer.length, length, position );
      //   if ( !length ) {
      //     return cb(0);
      //   }
      //   var lfd = fileDescriptors
      //     .filter(function(kfd) {
      //       return kfd.id == fd
      //     })
      //     .shift();
      //   streamToBuffer(
      //     lfd.fo.createReadStream({ start: position, end: Math.min( position+length, parseInt(lfd.fo.metadata.size) ) }),
      //     function(err, buf) {
      //       if ( err ) throw err;
      //       console.log(err,buf.length);
      //       console.log(buffer.length);
      //       buffer.write(buf.toString());
      //       console.log(buffer.length);
      //       cb(buf.length);
      //     }
      //   )
      // },
      // write: function(path, fd, buffer, length, position, cb) {
      //   console.log('WRITE',path,fd,length,position)
      //   cb();
      // },
      // release: function(path, fd, cb){
      //   console.log('RELEASE',path,fd);
      //   fileDescriptors = fileDescriptors.filter(function(lfd) {
      //     return lfd.id != fd;
      //   });
      //   cb(0);
      // },
      // releasedir: function(path, fd, cb) {
      //   console.log('RELEASEDIR',path,fd);
      //   cb();
      // },
      // create: function(path, mode, cb) {
      //   console.log('CREATE',path,mode);
      //   cb();
      // },
      // utimens: function(path, atime, mtime, cb) {
      //   console.log('UTIMENS',path,atime,mtime);
      //   cb();
      // },
      // unlink: function(path, cb) {
      //   console.log('UNLINK',path);
      //   cb();
      // },
      // rename: function(src, dest, cb) {
      //   console.log('RENAME',src,dest);
      //   cb();
      // },
      // link: function(src, dest, cb) {
      //   console.log('LINK',src,dest);
      //   cb();
      // },
      // symlink: function(src, dest, cb) {
      //   console.log('SYMLINK',src,dest);
      //   cb();
      // },
      // mkdir: function(path, mode, cb) {
      //   console.log('MKDIR',path,mode);
      //   cb();
      // },
      // rmdir: function(path, cb) {
      //   console.log('RMDIR',path);
      //   cb();
      // },
      // destroy: function(cb) {
      //   console.log('DESTROY');
      //   cb();
      // }
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
      clearTimeout(cacheCleanTO);
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
