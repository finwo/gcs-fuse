var fuse           = require('fuse-bindings'),
    fs             = require('fs'),
    mime           = require('mime-types'),
    stat           = require('./fixtures/stat'),
    Storage        = require('@google-cloud/storage'),
    streamToBuffer = require('stream-to-buffer'),
    tmp            = require('tmp'),
    Promise        = require('bluebird'),
    File           = Storage.File;

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
        cache[key] = { expires: (new Date().getTime()) + 250,
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
          console.log('- ' + key);
        } else {
          nextExpires = Math.min( nextExpires, cache[key].expires ) || cache[key].expires;
        }
      });
      nextExpires = nextExpires || ( tstamp + 2000 );
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
          var fd = { id: 5, mode: flags, attr: attr, fo: attr.fileObject };
          fileDescriptors.forEach(function(lfd) {
            fd.id = Math.max( fd.id, lfd.id + 1 );
          });
          fileDescriptors.push(fd);
          return cb(0,fd.id);
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
        if(lfd.mode.substr(0,1)!='r') return cb(fuse.EBADF);
        if ( ( lfd.fo.metadata.size === 0 ) && ( len > 0 ) ) {
          return cb(0);
        }
        streamToBuffer(
          lfd.fo.createReadStream({ start: pos, end: Math.min( pos+len, parseInt(lfd.fo.metadata.size) - pos ) }),
          function( err, buffer ) {
            if(err && (err.code===416)) return cb(0);
            if(err)return f_read(path,fd,buf,len,pos,cb,tries+1);
            buf.write(buffer.toString());
            cb(buffer.length);
          }
        );
      },

      create: function f_create( path, flags, cb, tries ) {
        tries = tries || 0;
        console.log('CREATE', tries, path, flags);
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        var file    = new File( bucket, path ),
            tmpFile = tmp.tmpNameSync();
        fs.writeFileSync(tmpFile,'');
        bucket.upload( tmpFile, {
          destination: file,
          metadata   : {
            contentType: mime.lookup( path )
          }
        }, function(err, fileObject) {
          fs.unlinkSync(tmpFile);
          if (err) return cb(fuse.EIO);
          cb(0,0);
        })
      },

      write: function f_write( path, fd, buf, len, pos, cb, tries ) {
        tries = tries || 0;
        console.log('WRITE', tries, path, fd, len, pos );
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        var lfd = fileDescriptors.filter(function(lfdo) {
          return lfdo.id == fd;
        }).shift();
        if(!lfd)return cb(fuse.ENOENT);
        streamToBuffer(
          lfd.fo.createReadStream(),
          function(err, oldContents) {
            var ws = lfd.fo.createWriteStream();
            ws.on('error', function(err) {cb(fuse.EIO)});
            ws.on('finish', function(err) {cb(len)});
            ws.write(oldContents.slice(0,pos));
            ws.write(buf.slice(0,len));
            ws.end(oldContents.slice(pos+len));
          }
        )
      },

      truncate: function( path, size, cb, tries ) {
        tries = tries || 0;
        console.log('TRUNCATE',tries,path,size);
        ops.getattr ( path, function(err,attr) {
          streamToBuffer(
            attr.fileObject.createReadStream({ start: 0, end: size }),
            function( err, buffer ) {
              attr.fileObject.createWriteStream()
                .on('error', function(err) {cb(fuse.EIO)})
                .on('finish', function(err) {cb(0);})
                .end(buffer);
            }
          );
        }, true );
      },

      unlink: function(path, cb, tries) {
        tries = tries || 0;
        console.log('UNLINK',tries,path);
        ops.getattr ( path, function(err,attr) {
          if(err)return cb(fuse.EIO);
          attr.fileObject.delete(function() {
            cb(0);
          });
        }, true );
      },

      mkdir: function( path, mode, cb, tries ) {
        tries = tries || 0;
        console.log('MKDIR',tries,path,mode);
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        if ( path.slice(-1) != '/') path += '/';
        var file   = new File( bucket, path );
        file.metadata.contentType = 'Folder';
        file.save('',function(err,ff) {
          if(err)return cb(fuse.EIO);
          cb(0);
        });
      },

      rmdir: function( path, cb, tries ) {
        tries = tries || 0;
        console.log('RMDIR',tries,path);
        ops.unlink(path,cb);
      },

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
