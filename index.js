#!/usr/bin/env node

var debug          = require('debug')('gcs-fuse'),
    fuse           = require('fuse-bindings'),
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
    fileDescriptors = [],
    unlinked        = []; // List of recently unlinked files due to cache

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

function f_bucket_getfiles(prefix) {
  return new Promise(function(resolve, reject) {
    var tstamp = new Date().getTime(),
        key    = 'f_bucket_getfiles',
        opts   = undefined;
    if (prefix) opts = { prefix: prefix };
    bucket.getFiles(opts)
      .then(function(result) {
        result = result.shift();
        resolve(result);
      })
      .catch(reject);
  });
}

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
        debug('GETATTR',tries,path);
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
          if (!fileObject) {
            debug('- ENOENT');
            return cb(fuse.ENOENT);
          }
          var attr = { mode : 'file',
                       size : parseInt(fileObject.metadata.size),
                       mtime: Date.parse( fileObject.metadata.updated ),
                       ctime: Date.parse( fileObject.metadata.updated ) };
          if ( fileObject.metadata.metadata && fileObject.metadata.metadata.gcsfuse_symlink_target ) {
            attr.mode = 'link';
          }
          if ( fileObject.name.slice(-1) === '/' ) {
            attr.mode = 'dir';
            attr.size = 4096;
          }
          debug('-',attr.mode,attr.size);
          if ( objMode ) {
            attr.fileObject = fileObject;
            return cb( null, attr );
          }
          cb(null,stat(attr));
        }, true, 0);
      },

      readdir: function f_readdir( path, cb, objMode, tries ) {
        tries = tries || 0;
        objMode = objMode || false;
        if(tries>10)return cb(fuse.EIO);
        debug('READDIR',tries,objMode,path);
        if ( path.slice(-1) != '/' ) path += '/';
        while ( path.substr(0,1) === '/' ) path = path.substr(1);
        f_bucket_getfiles( path )
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
            cb(null,['.','..'].concat(list));
          })
          .catch(function(err) {
            debug(err);
            f_readdir( path, cb, objMode, tries + 1 );
          })
      },

      open: function f_open( path, flags, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        flags = ( 'string' === typeof flags ) ? flags : flagDecode( flags );
        debug('OPEN',tries,path,flags);

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
        debug('RELEASE',tries,fd,path);
        fileDescriptors = fileDescriptors.filter(function(lfd) {
          return lfd.id != fd;
        });
        cb(0);
      },

      read: function f_read( path, fd, buf, len, pos, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('READ',tries,fd,pos,len,path);
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
        if(tries>10)return cb(fuse.EIO);
        debug('CREATE', tries, path, flags);
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        var file    = new File( bucket, path ),
            tmpFile = tmp.tmpNameSync();
        fs.writeFileSync(tmpFile,'');
        bucket.upload( tmpFile, {
          destination: file,
          metadata   : {
            contentType: mime.lookup( path ) || 'application/octet-stream',
          }
        }, function(err, fileObject) {
          fs.unlinkSync(tmpFile);
          if (err) return f_create( path, flags, cb, tries + 1 );
          ops.open( path, flags, cb );
        })
      },

      write: function f_write( path, fd, buf, len, pos, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('WRITE', tries, path, fd, len, pos );
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        var lfd = fileDescriptors.filter(function(lfdo) {
          return lfdo.id == fd;
        }).shift();
        if(!lfd)return cb(fuse.ENOENT);
        streamToBuffer(
          lfd.fo.createReadStream(),
          function(err, oldContents) {
            if (err) return f_write( path, fd, buf, len, pos, cb, tries + 1 );
            var ws = lfd.fo.createWriteStream();
            ws.on('error', function(err) {
              debug('-',err);
              cb(fuse.EIO);
            });
            ws.on('finish', function(err) {cb(len)});
            ws.write(oldContents.slice(0,pos));
            ws.write(buf.slice(0,len));
            ws.end(oldContents.slice(pos+len));
          }
        )
      },

      truncate: function f_truncate( path, size, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('TRUNCATE',tries,path,size);
        ops.getattr ( path, function(err,attr) {
          streamToBuffer(
            attr.fileObject.createReadStream({ start: 0, end: size }),
            function( err, buffer ) {
              attr.fileObject.createWriteStream()
                .on('error', function(err) {
                  f_truncate( path, size, cb, tries + 1 );
                })
                .on('finish', function(err) {
                  cb(0);
                })
                .end(buffer);
            }
          );
        }, true );
      },

      unlink: function f_unlink(path, cb, tries) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('UNLINK',tries,path);
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        var fileObject = bucket.file(path);
        if(!fileObject) return cb(fuse.ENOENT);
        fileObject
          .delete()
          .then(function() {
            cb(0);
          })
          .catch(function(err) {
            f_unlink(path,cb,tries+1);
          });
      },

      mkdir: function f_mkdir( path, mode, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('MKDIR',tries,path,mode);
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        if ( path.slice(-1) != '/') path += '/';
        var file    = new File( bucket, path ),
            tmpFile = tmp.tmpNameSync();
        fs.writeFileSync(tmpFile,'');
        bucket.upload( tmpFile, {
          destination: file
        }, function(err, fileObject) {
          fs.unlinkSync(tmpFile);
          if (err) return f_mkdir( path, mode, cb, tries + 1 );
          cb(0);
        });
      },

      rmdir: function f_rmdir( path, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('RMDIR',tries,path);
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        if ( path.slice(-1) != '/') path += '/';
        ops.unlink(path,function(err) {
          if(err)return f_rmdir( path, cb, tries + 1 );
          cb(0);
        });
      },

      symlink: function f_symlink( target, path, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('SYMLINK',tries,path,'=>',target);
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        var file    = new File( bucket, path ),
            tmpFile = tmp.tmpNameSync();
        fs.writeFileSync(tmpFile,'');
        bucket.upload( tmpFile, {
          destination: file,
          metadata: {
            metadata: {
              gcsfuse_symlink_target: target
            }
          }
        }, function(err, fileObject) {
          if(err)return f_symlink( target, path, cb, tries + 1 );
          cb(0);
        });
      },

      readlink: function f_readlink( path, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('READLINK',tries,path);
        ops.getattr(path,function(err, attr) {
          if(err)return f_readlink(path,cb,tries+1);
          if(attr.mode !== 'link') return cb(fuse.ENOENT);
          cb(0,attr.fileObject.metadata.metadata.gcsfuse_symlink_target);
        }, true);
      },

      fgetattr: function( path, fd, cb ) {
        debug('FGETATTR',fd,path);
        if ( path.substr(0,1) === '/' ) path = path.substr(1);
        var lfd = fileDescriptors.filter(function(lfdo) {
          return lfdo.id == fd;
        }).shift();
        if(!lfd)return cb(fuse.ENOENT);
        var fileObject = lfd.fo,
            attr       = { mode : 'file',
                     size : parseInt(fileObject.metadata.size),
                     mtime: Date.parse( fileObject.metadata.updated ),
                     ctime: Date.parse( fileObject.metadata.updated ) };
        if ( fileObject.metadata.metadata && fileObject.metadata.metadata.gcsfuse_symlink_target ) {
          attr.mode = 'link';
        }
        if ( fileObject.name.slice(-1) === '/' ) {
          attr.mode = 'dir';
          attr.size = 4096;
        }
        cb(0,stat(attr));
      },

      fsync: function( path, fd, datasync, cb ) {
        debug('FSYNC',fd,path);
        cb(0);
      },

      rename: function f_rename( src, dest, cb, tries ) {
        tries = tries || 0;
        if(tries>10)return cb(fuse.EIO);
        debug('RENAME',tries,src,dest);
        if ( dest.substr(0,1) === '/' ) dest = dest.substr(1);
        ops.getattr(src,function(err,attr) {
          if(err)f_rename(src,dest,cb,tries+1);
          if( ( attr.mode === 'dir' ) && ( dest.slice(-1) !== '/' ) ) dest += '/';
          attr.fileObject
            .move(dest)
            .then(function() {
              cb(0);
            })
            .catch(function(err) {
              f_rename(src,dest,cb,tries+1);
            })
        },true);
      },

      chmod: function( path, mode, cb ) {
        debug('CHMOD',path,mode);
        cb(0);
      },

      chown: function( path, uid, gid, cb ) {
        debug('CHOWN',path,uid,gid);
        cb(0);
      },

      utimens: function( path, atime, mtime, cb ) {
        debug('UTIMENS',path,atime,mtime);
        cb(0);
      },

    }, function(err) {
      if(err) throw err;
      debug( bucket.id + ' mounted on ' + path );
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
      debug( path + ' unmounted');
    }
  })
});
