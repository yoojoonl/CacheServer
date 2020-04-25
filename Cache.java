/*
  Cache code for yoojoonl
  Implements an LRU cache that stores whole files using concurrent hash maps and lists.
*/

import java.io.*;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.ArrayList;

public class Cache {
  private ConcurrentHashMap<String, File> files;
  private ConcurrentHashMap<String, Long> versions;
  private ArrayList<String> reading;
  public LinkedList<String> queue;
  public long size;
  public long limit;
  public Cache(long cache_size){
    files = new ConcurrentHashMap<String, File>(); 
    versions = new ConcurrentHashMap<String, Long>();
    reading = new ArrayList<String>();
    queue = new LinkedList<String>(); 
    limit = cache_size;
    size = 0;
  }
  //Used to get version of a file in the cache
  public synchronized long get_version(String path){
    if(versions.containsKey(path)) return versions.get(path);
    return -1;
  }
  //Adds a file to the list of files being currently read 
  public synchronized void add_reading(String path){
    if(!reading.contains(path)){
      reading.add(path);
    }
  }
  //If in the list of read files removes it
  public synchronized void remove_reading(String path){
    if(reading.contains(path)){
      reading.remove(path);
    }
  }
  //Returns the size of the cache
  public synchronized long get_size(){
    return size;
  }
  //Returns the first file in the cache currently
  public synchronized String peek(){
    return queue.peekFirst();
  }
  //Used for debugging to print everything in the cache
  public synchronized void print_cache(){
    int i = 0;
    while(i < queue.size()){
      System.err.println("cache:" + i + ":" + queue.get(i));
      i++;
    }
  }
  //Used for debugging to print everything in the list of files being read
  public synchronized void print_reading(){
    int i = 0;
    while(i < reading.size()){
      System.err.println("reading:" + i + ":" + reading.get(i));
      i++;
    }
  }
  //Removes a file from its place in the queue and adds it to the end of the queue
  public synchronized void update_order(String path){
    if(files.containsKey(path)){
      queue.remove(path);
      queue.addLast(path);
    }
  }
  //Removes file from the queue in LRU order until there is enough space for our new file
  public synchronized Boolean make_space(String path, long f_size){
    int current = 0;
    while((f_size + size > limit) && (queue.size() != 0) && (current < queue.size())){
      if(reading.contains(queue.get(current))){
        current += 1;
      }
      else{
        String temp_path = queue.remove(current);
        File f = files.remove(temp_path);
        versions.remove(temp_path);
        size -= f.length();
        f.delete();
      }    
    }
    if(f_size + size <= limit){
      return true;
    }
    return false;
  }

  //Removes a file from the cache
  public synchronized Boolean remove(String path){
    if(!files.containsKey(path)) return false;
    if(reading.contains(path)) return false;
    reading.remove(path);
    File f = files.remove(path);
    f.delete();
    versions.remove(path);
    queue.remove(path);
    return true;
  }
  //Updates a version of a file in the cache
  public synchronized void update_version(String path, long version){
    if(versions.containsKey(path)){
      versions.replace(path, version);
    }
  }
  //Adds a new file to the cache with version
  public synchronized void add(File f, String path, long version) throws IOException{
    if(files.containsKey(path)){
      byte[] content = new byte[(int)f.length()];
      RandomAccessFile temp = new RandomAccessFile(f, "rw");
      temp.read(content);
      File f_cache = new File(path);
      temp = new RandomAccessFile(f_cache,"rw");
      temp.write(content);
      temp.close();
      files.replace(path, f_cache);
      versions.replace(path, version);
      queue.remove(path);
      queue.addLast(path);
    }
    else{
      if(f.length() > limit){
        f.delete();
        return;
      }
      Boolean is_space = true;
      if(f.length() + size > limit){
        is_space = make_space(path, f.length());
      }
      if(!is_space){
        System.err.println("Not enough space in cache for file!");
        f.delete();
      }
      else{
        byte[] content = new byte[(int)f.length()];
        RandomAccessFile temp = new RandomAccessFile(f, "rw");
        temp.read(content);
        File f_cache = new File(path);
        if(f_cache.exists()){
          f_cache.delete();
          f_cache = new File(path);
          f_cache.createNewFile();
        }
        else{
          f_cache.createNewFile();
        }
        temp = new RandomAccessFile(f_cache, "rw");
        temp.write(content);
        temp.close();
        size += f_cache.length();
        files.put(path, f_cache);
        versions.put(path, version);
        queue.addLast(path);
      }   
    }
  }
  //Checks if file is in the cache and returns the file if it exists
  public synchronized File getF(String path){
    if(path == null){
      return null;
    }
    if(files.containsKey(path)){
      File f = files.get(path);
      return f;
    }
    return null;
  }
}