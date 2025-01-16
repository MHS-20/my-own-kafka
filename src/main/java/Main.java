import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    System.err.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 9092;
    
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      // Wait for connection from client
      clientSocket = serverSocket.accept();

      // get correlation_id from client
      byte[] correlation_id = new byte[4]; 
      byte[] other_fields = clientSocket.getInputStream().readNBytes(8);
      clientSocket.getInputStream().read(correlation_id);

      clientSocket.getOutputStream().write(new byte[] {0, 0, 0, 0});
      clientSocket.getOutputStream().write(correlation_id);

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
