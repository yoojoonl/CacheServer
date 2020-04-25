/* 
  Server code for yoojoonl
  Connects to Proxy.java. Takes files on the server and sends them to proxy on request. And on request
  from proxy can create or write to files on the server.
*/

import java.io.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.Naming;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.util.concurrent.ConcurrentHashMap;


public class Server extends UnicastRemoteObject implements ServerI{
  private static String root_dir;
  private ConcurrentHashMap<String, Long> versions = new ConcurrentHashMap<String, Long>();;
  public Server() throws RemoteException {
    super();
  }
  //Used to delete files from the server
  public synchronized int unlinkF(String path){
    String f_path = root_dir + "/" + path;
    File f = new File(f_path);
    if(!f.exists()){
      return -1;
    }
    else if(f.isDirectory()){
      return -1;
    }
    f.delete();
    return 0;
  }
  //Returns the current version of the file on the server
  public long get_version(String path){
    if(versions.containsKey(path)){
      return versions.get(path);
    }
    return (long)-1;
  }
  //Returns the file size of the file on the server
  public long file_size(String path){
    String f_path = root_dir + "/" + path;
    File f = new File(root_dir + "/" + path);
    if(f.exists()){
      return f.length();
    }
    return -1;
  }
  //Returns boolean based on wheter the file exists in the server
  public boolean file_exists(String path){
    File f = new File(root_dir + "/" + path);
    if(f.exists()){
      return true;
    }
    return false;
  }
  //Creates a new file on the server
  public void create_file(String path){
    String f_path = root_dir + "/" + path;
    File f = new File(f_path);
  }

  //Gets a file from the server and returns the contents of the file
  public byte[] getF(String path, long offset, int size) {
    System.err.println("Server Get");
    byte[] r = null;
    String f_path = root_dir + "/" + path;
    File f = new File(f_path);
    RandomAccessFile rf = null;
    if(!f.exists()){
      System.err.println("no such file");
      return null;
    }
    try{
      rf = new RandomAccessFile(f, "r");
    }
    catch(FileNotFoundException e){
      e.printStackTrace();
    }
    try{
      r = new byte[size];
      rf.seek(offset);
      rf.read(r);
    }
    catch(IOException e){
      e.printStackTrace();
    }
    if(!versions.containsKey(path)){
      versions.put(path, (long)0);
    }
    return r;
  }

  //Takes in a byte[] buf array and writes to a file on the server. Changes version number when done is set to true
  public void writeF(String path, byte[] buf, long offset, Boolean done) {
    String f_path = root_dir + "/" + path;
    File f = new File(f_path);
    RandomAccessFile rf = null;
    try{
      rf = new RandomAccessFile(f, "rw");
    }
    catch(FileNotFoundException e){
      e.printStackTrace();
    }
    try{
      rf.seek(offset);
      rf.write(buf);
    }
    catch(IOException e){
      e.printStackTrace();
    }
    if(done == true){
      if(versions.containsKey(path)){
        System.err.println("Server write");
        versions.replace(path, versions.get(path) + 1);
    }
      else{
        versions.put(path, (long)0);
      }
    }
  }
  //Used to create the server that the proxy can then connect to
  public static void main(String [] args) throws RemoteException{
    if(args.length != 2) return;
    int port = Integer.parseInt(args[0]);
    String root = args[1];
    File f = new File(root);
    try{
      root_dir = f.getCanonicalPath();
    }
    catch(IOException e){
      e.printStackTrace();
    }
    LocateRegistry.createRegistry(port);
    Server server = new Server();
    System.err.println("127.0.0.1:" + port + "/server");
    try{
      Naming.rebind("//127.0.0.1:" + port + "/server", server);
    }
    catch(MalformedURLException e){
      e.printStackTrace();
    }
    System.err.println("Server on");
  }
}

