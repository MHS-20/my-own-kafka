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
      byte[] error_code = new byte[2];
      byte[] max_version = new byte[2];
      short apiV = ByteBuffer.wrap(request_api_version).getShort();
      short apiK = ByteBuffer.wrap(request_api_key).getShort();

      if (apiV > 4)
        error_code = new byte[] { 0, 35 };
      else
        error_code = new byte[] { 0, 0 };

      if (apiK == 18)
        max_version = new byte[] { 0, 4 };
      else
        max_version = new byte[] { 0, 3 };

      byte[] min_version = new byte[] { 0, 3 };
      byte[] tagged_fields = new byte[] { 0 };
      byte[] throttle_time_ms = new byte[] { 0, 0, 0, 0 };

      int messageSizeInt = correlation_id.length +
          request_api_key.length +
          request_api_version.length +
          error_code.length +
          max_version.length +
          min_version.length +
          tagged_fields.length +
          throttle_time_ms.length + 
          tagged_fields.length;

      System.out.println("messageSizeInt: " + messageSizeInt);
      message_size = ByteBuffer.allocate(4).putInt(messageSizeInt).array();
      clientSocket.getOutputStream().write(message_size);
      clientSocket.getOutputStream().write(correlation_id);
      clientSocket.getOutputStream().write(error_code);
      clientSocket.getOutputStream().write(request_api_key);
      clientSocket.getOutputStream().write(request_api_version);

      clientSocket.getOutputStream().write(min_version);
      clientSocket.getOutputStream().write(max_version);
      clientSocket.getOutputStream().write(tagged_fields);
      clientSocket.getOutputStream().write(throttle_time_ms);
      clientSocket.getOutputStream().write(tagged_fields);

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
