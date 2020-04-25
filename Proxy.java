/* Proxy code for yoojoonl
   Connects to Server.java and uses Cache.java to recieve files from the server and cache them 
   Uses check on use open close semantics
   */

import java.io.*;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class Proxy {
  private static AtomicInteger global_fd = new AtomicInteger(100);
  private static String server_ip;
  private static int server_port;
  private static String cache_dir;
  private static long cache_size;
  private static String cache_root;
  static ServerI server;
  private static Cache cache;
  private static HashMap<Integer, RandomAccessFile> fd_file = new HashMap<Integer, RandomAccessFile>();
  private static HashMap<Integer, File> f_file = new HashMap<Integer, File>();
  private static HashMap<Integer, String> fd_path = new HashMap<Integer, String>();
  private static HashMap<Integer, Boolean> updated = new HashMap<Integer, Boolean>();
  private static ReentrantReadWriteLock read_write_lock = new ReentrantReadWriteLock();

  private static class FileHandler implements FileHandling {
    //Formats paths to remove extra characters in front and handles .. to access earlier file
    public String format(String path){
      if((path.length() == 1) && (path.charAt(0) != '.') && (path.charAt(0) != '/')){
        return path;
      }
      if(path.substring(0,2) == ".."){
        return null;
      }
      if(path.charAt(0) == '.'){
        path = path.substring(1,path.length());
      }
      if(path.charAt(0) == '/'){
        path = path.substring(1,path.length());
      }
      while(path.contains("..")){
        int i = path.indexOf("..");
        int i1 = -1;
        int i2 = 0;
        for(int j = i; j >= 0; j--){
          if(path.charAt(j) == '/'){
            if(i1 == -1){
              i1 = j;
            }
            else{
              i2 = j;
              break;
            }
          }
        }
        path = path.substring(0,i2) + path.substring(i+2,path.length());
      }
      if(path.charAt(0) == '/'){
        path = path.substring(1,path.length());
      }
      return path;
    }

    //Sends file to server in chunks of max size 1,000,000
    public synchronized void to_server(String path, RandomAccessFile rf){
      long file_size = 0;
      Boolean done = false;
      try{
        file_size = rf.length();
      }
      catch(IOException e){
        e.printStackTrace();
      }
      long max = 1000000;
      long runs = file_size / max;
      long offset = 0;
      if(file_size % max != 0){
        runs += 1;
      }
      long current = 1;
      while(current <= runs){
        byte[] content;
        long write_size = max;
        if(current == runs){
          content = new byte[(int)file_size];
          write_size = file_size;
          done = true;
        }
        else{
          content = new byte[(int)max];
          file_size -= max;
        }
        try{
          rf.seek(offset);
          rf.read(content);
          server.writeF(path, content, offset, done);
        }
        catch(RemoteException e){
          e.printStackTrace();
        }
        catch(IOException e){
          e.printStackTrace();
        }     
        offset += write_size;
        current += 1;
      }
    }
    //Recieves files from server in chunks of max size 1,000,000
    public synchronized void from_server(String path, RandomAccessFile rf) {
      long file_size = 0;
      try{
        file_size = server.file_size(path);
      }
      catch(RemoteException e){
        e.printStackTrace();
      }
      long max = 1000000;
      long runs = file_size / max;
      long offset = 0;
      if(file_size % max != 0){
        runs += 1;
      }
      long current = 1;
      while(current <= runs){
        byte[] content;
        long write_size = max;
        if(current == runs){
          content = new byte[(int)file_size];
          write_size = file_size;
        }
        else{
          content = new byte[(int)max];
          file_size -= max;
        }
        try{
          content = server.getF(path,offset, content.length);
          rf.seek(offset);
          rf.write(content);
        }
        catch(RemoteException e){
          e.printStackTrace();
        }
        catch(IOException e){
          e.printStackTrace();
        }     
        offset += write_size;
        current += 1;
      }
    }
    //Function called to open files with 4 different options.
    //If in cache checks for updates then reads from cache
    //If not in cache recieves data from server then adds to cache
    public int open( String path, OpenOption o ) {
      path = format(path);
      System.err.println("Proxy open:" + path);
      if(path == null) return Errors.ENOENT;
      String temp = path.replace('/', '_');
      read_write_lock.readLock().lock();
      String cache_path = cache_root + "/" + temp;
      Boolean hit = true;
      Boolean on_server = true;
      long server_version = -1;
      File f = cache.getF(cache_path);
      RandomAccessFile rf = null;
      int ret_fd = 0;
      if(f == null){
        try{
          on_server = server.file_exists(path);
        }
        catch(RemoteException e){
          read_write_lock.readLock().unlock();
          e.printStackTrace();
        }
        hit = false;
        f = new File(temp);
        if(f.exists()){
          f.delete();
          f = new File(temp);
        }
        if(on_server == true){
          try{
            rf = new RandomAccessFile(f,"rw");
            from_server(path, rf);
          }
          catch(FileNotFoundException e){
            read_write_lock.readLock().unlock();
            e.printStackTrace();
          }
        }
      }
      try{
        server_version = server.get_version(path);
      }
      catch(RemoteException e){
        read_write_lock.readLock().unlock();
        e.printStackTrace();
      }
      if((hit == true) && (server_version != -1) && (server_version != cache.get_version(cache_path))){
        try{
          rf = new RandomAccessFile(f,"rw");
        }
        catch(FileNotFoundException e){
          read_write_lock.readLock().unlock();
          e.printStackTrace();
        }
        from_server(path, rf);
        try{
          cache.add(f, cache_path, server_version);
        }
        catch(IOException e){
          e.printStackTrace();
        }

        
      }
      else if(hit == true){
        cache.update_order(cache_path);
      }
      try{
        switch(o) {
          case CREATE:
            if(f.isDirectory()){
              read_write_lock.readLock().unlock();
              return Errors.EISDIR;
            } 
            rf = new RandomAccessFile(f, "rw");
            if((hit == false) && (server_version == -1)){
              cache.add(f, cache_path, 0);
            }
            else if((hit == false) && (server_version != -1)){
              cache.add(f, cache_path, server_version);
            }
            if(on_server == false){
              server.create_file(path);
            }
            fd_file.put(global_fd.get(), rf);
            f_file.put(global_fd.get(), f);
            fd_path.put(global_fd.get(), path);
            updated.put(global_fd.get(), false);
            cache.add_reading(cache_path);
            ret_fd = global_fd.get();
            global_fd.incrementAndGet();
            read_write_lock.readLock().unlock();
            return ret_fd;
          case CREATE_NEW:
            if(f.isDirectory()){
              read_write_lock.readLock().unlock();
              return Errors.EISDIR;
            } 
            if(f.exists()){
              read_write_lock.readLock().unlock();
              return Errors.EEXIST;
            } 
            rf = new RandomAccessFile(f, "rw");
            if(on_server == true || hit == true){
              f.delete();
              read_write_lock.readLock().unlock();
              return -1;
            }
            if(hit == false){
              cache.add(f, cache_path, 0);
            }
            server.create_file(path);
            fd_file.put(global_fd.get(), rf);
            f_file.put(global_fd.get(), f);
            fd_path.put(global_fd.get(), path);
            updated.put(global_fd.get(), false);
            cache.add_reading(cache_path);
            ret_fd = global_fd.get();
            global_fd.incrementAndGet();
            read_write_lock.readLock().unlock();
            return ret_fd;
          case READ:
            if(!f.exists()){
              read_write_lock.readLock().unlock();
              return Errors.ENOENT;
            } 
            if(f.isDirectory()){
              fd_path.put(global_fd.get(), path);
              ret_fd = global_fd.get();
              global_fd.incrementAndGet();
              read_write_lock.readLock().unlock();
              return ret_fd;
            }
            rf = new RandomAccessFile(f, "r");
            if(on_server == false){
              f.delete();
              read_write_lock.readLock().unlock();
              return -1;
            }

            if(hit == false){
              cache.add(f, cache_path, server_version);
            }
            fd_file.put(global_fd.get(), rf);
            f_file.put(global_fd.get(), f);
            fd_path.put(global_fd.get(), path);
            updated.put(global_fd.get(), false);
            cache.add_reading(cache_path);
            ret_fd = global_fd.get();
            global_fd.incrementAndGet();
            read_write_lock.readLock().unlock();
            return ret_fd;

          case WRITE:
            if(!f.exists()){
              read_write_lock.readLock().unlock();
              return Errors.ENOENT;
            } 
            if(!f.canWrite()){
              read_write_lock.readLock().unlock();
              return Errors.EINVAL;
            } 
            if(f.isDirectory()){
              read_write_lock.readLock().unlock();
              return Errors.EISDIR;
            } 
            rf = new RandomAccessFile(path, "rw");
            if(on_server == false){
              f.delete();
              read_write_lock.readLock().unlock();
              return -1;
            }
            if(hit == false){
              cache.add(f, cache_path, server_version);
            }
            fd_file.put(global_fd.get(), rf);
            f_file.put(global_fd.get(), f);
            fd_path.put(global_fd.get(), path);
            updated.put(global_fd.get(), false);
            cache.add_reading(cache_path);
            ret_fd = global_fd.get();
            global_fd.incrementAndGet();
            read_write_lock.readLock().unlock();
            return ret_fd;
          default:
            read_write_lock.readLock().unlock();
            return Errors.EINVAL;
        }
      }
      catch(IOException e){
        read_write_lock.readLock().unlock();
        return Errors.ENOSYS;
      }
    }

    //Closes file opened and if changes have been made updates on the cache and server
    public int close( int fd ) {
      read_write_lock.writeLock().lock();
      System.err.println("Proxy Close");
      if(!fd_file.containsKey(fd)){
        read_write_lock.writeLock().unlock();
        return Errors.EBADF;
      } 
      String path = fd_path.get(fd);
      String temp = path.replace('/', '_');
      String cache_path = cache_root + "/" + temp;
      RandomAccessFile rf = fd_file.get(fd);
      File f = f_file.get(fd);
      if(!f.exists()) {
        read_write_lock.writeLock().unlock();
        return Errors.ENOENT;
      }
      if(updated.get(fd) == true){
        to_server(path, rf);
        long new_version = -2;
        try{
          new_version = server.get_version(path);
        }
        catch(RemoteException e){
          e.printStackTrace();
        }
        System.err.println("New Version:" + new_version);
        updated.replace(fd, false);
        try{
          cache.add(f, cache_path, new_version);
        }
        catch(IOException e){
          e.printStackTrace();
        }
      }
      if(f.isDirectory()){
        fd_path.remove(fd);
        fd_file.remove(fd);
        f_file.remove(fd);
        cache.remove_reading(cache_path);
        read_write_lock.writeLock().unlock();
        return 0;
      }
      try {
        rf.close();
        fd_path.remove(fd);
        fd_file.remove(fd);
        f_file.remove(fd);
        cache.remove_reading(cache_path);
        read_write_lock.writeLock().unlock();
        return 0;
      }
      catch(IOException e){
        read_write_lock.writeLock().unlock();
        return Errors.EBADF;
      }
    }

    //Writes to the file locally but doesn't push anything to cache or server yet and returns how much written
    public long write( int fd, byte[] buf ) {
      if(buf == null){
        return Errors.EINVAL;
      }
      if(!fd_file.containsKey(fd)){
        return Errors.EBADF;
      }
      if(!fd_path.containsKey(fd)){
        return Errors.EBADF;
      }
      System.err.println("proxy write");
      String path = fd_path.get(fd);
      File f = f_file.get(fd);
      if(f.isDirectory()) {return Errors.EISDIR;}
      if(!f.exists()){return Errors.ENOENT;}
      RandomAccessFile rf = fd_file.get(fd);
      try{
        rf.write(buf);
      } 
      catch(IOException e){
        return Errors.EBADF;
      }
      fd_file.replace(fd, rf);
      updated.replace(fd, true);
      return buf.length;
    }

    //Reads opened file and returns a long of how much has been read
    public long read( int fd, byte[] buf ) {
      if(!fd_path.containsKey(fd)) return Errors.EBADF;
      RandomAccessFile rf = fd_file.get(fd);
      String path = fd_path.get(fd);
      File f = f_file.get(fd);
      if(!f.exists()) return Errors.ENOENT;
      if(f.isDirectory()) return Errors.EISDIR;
      if(!f.canRead()) return Errors.EBADF;
      try{
        int read_int = rf.read(buf);
        if(read_int == -1) return 0;
        return (long)read_int;
      }
      catch(IOException e){
        return Errors.EBADF;
      }
    }

    //Repositions file offest
    public long lseek( int fd, long pos, LseekOption o ) {
      System.err.println("proxy lseek");
      if(!fd_file.containsKey(fd)) return Errors.EBADF;
      RandomAccessFile rf = fd_file.get(fd);
      String path = fd_path.get(fd);
      File f = f_file.get(fd);
      if(!f.exists()) return Errors.ENOENT;
      if(f.isDirectory()) return Errors.EISDIR;
      try{
        switch(o){
          case FROM_CURRENT:
            pos = rf.getFilePointer() + pos;
          case FROM_START:
            break;
          case FROM_END:
            pos = rf.length() + pos;
          default:
            return Errors.EINVAL;
        }
      }
      catch(IOException e){
        return Errors.EINVAL;
      }
      if(pos < 0) return Errors.EINVAL;
      try{
        rf.seek(pos);
        return rf.getFilePointer();
      }
      catch(IOException e){
        return Errors.EBADF;
      }
    }

    //Removes a file from cache and server
    public int unlink( String path ) {
      path = format(path);
      System.err.println("proxy unlink");
      String temp = path.replace('/', '_');
      String cache_path = cache_root + "/" + temp;
      if(path == null) return Errors.ENOENT;
      int r = 0;
      try{
        r = server.unlinkF(path);
        cache.remove(cache_path);
      }
      catch(RemoteException e){
        e.printStackTrace();
      }
      return r;
    }

    public void clientdone() {
      return;
    }
  }
  
  private static class FileHandlingFactory implements FileHandlingMaking {
    public FileHandling newclient() {
      return new FileHandler();
    }
  }
  //Creates the connection to Server.java
  public static void main(String[] args) throws IOException {
    if(args.length != 4) return;
    server_ip = args[0];
    server_port = Integer.parseInt(args[1]);
    cache_dir = args[2];
    cache_size = Integer.parseInt(args[3]);
    File f = new File(cache_dir);
    cache_root = f.getCanonicalPath();
    //Connects to server
    try{
      server = (ServerI) Naming.lookup("//" + server_ip + ":" + server_port + "/server");
    }
    catch(NotBoundException e){
      e.printStackTrace();
    }
    cache = new Cache(cache_size);
    (new RPCreceiver(new FileHandlingFactory())).run(); 
  }
}



