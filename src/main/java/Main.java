import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args) {
    System.err.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 9092;

    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      // Wait for connection from client
      clientSocket = serverSocket.accept();

      // get fields from request
      byte[] message_size = new byte[4];
      byte[] request_api_key = new byte[2];
      byte[] request_api_version = new byte[2];
      byte[] correlation_id = new byte[4];

      clientSocket.getInputStream().read(message_size);
      clientSocket.getInputStream().read(request_api_key);
      clientSocket.getInputStream().read(request_api_version);
      clientSocket.getInputStream().read(correlation_id);

      // check api version
      byte[] error_code; 
      int apiV = ByteBuffer.wrap(request_api_version).getInt();
      if (apiV > 4)
        error_code = new byte[] { 0, 23 };
      else
        error_code = new byte[] { 0, 0 };

      // send response
      message_size = new byte[] { 0, 0, 0, 4 };
      clientSocket.getOutputStream().write(message_size);
      clientSocket.getOutputStream().write(correlation_id);
      clientSocket.getOutputStream().write(error_code);

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
