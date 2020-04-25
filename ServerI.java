/*
Server Interface Code for yoojoonl
Used in server.java to be an interface for a server
*/

import java.rmi.Remote;
import java.rmi.RemoteException;
public interface ServerI extends Remote{
  public int unlinkF(String path) throws RemoteException;
  public long file_size(String path) throws RemoteException;
  public long get_version(String path) throws RemoteException;
  public boolean file_exists(String path) throws RemoteException;
  public void create_file(String path) throws RemoteException;
  public byte[] getF(String path, long offset, int size) throws RemoteException;
  public void writeF(String path, byte[] buf, long offset, Boolean done) throws RemoteException;
}