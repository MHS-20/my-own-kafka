import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;

public class Main {

    public static void main(String[] args) {
        System.err.println("Starting server...");
        final int port = 9092;
        int i = 0;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true)
                try {
                    Socket clientSocket = serverSocket.accept();

                    // thread handling the client
                    new Thread(() -> {
                        try (InputStream in = clientSocket.getInputStream();
                                OutputStream out = clientSocket.getOutputStream()) {

                            System.out.println("Client connected: " + clientSocket.getInetAddress());

                            // handle multiple requests from the same client
                            while (!clientSocket.isClosed()) {
                                System.out.println("Handling a request from: " + clientSocket.getInetAddress());
                                processClientRequest(in, out);
                            }

                        } catch (SocketException e) {
                            System.err.println("Socket closed: " + e.getMessage());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }).start();

                    // // deamon
                    // while (true) {
                    // processClientRequest(in, out);
                    // }

                } catch (IOException e) {
                    System.err.println("Client communication error: " + e.getMessage());
                    e.printStackTrace();
                }

        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    private static void processClientRequest(InputStream in, OutputStream out) throws IOException {
        // request size
        int size = ByteBuffer.wrap(in.readNBytes(4)).getInt();
        System.out.println("Request size: " + size);

        // Read the API key (16 bits) and API version (16 bits)
        byte[] apiKey = in.readNBytes(2);
        System.out.println("API key: " + ByteBuffer.wrap(apiKey).getShort());

        short apiVersion = ByteBuffer.wrap(in.readNBytes(2)).getShort();
        System.out.println("API version: " + apiVersion);

        // Read the correlation ID (32 bits)
        byte[] correlationId = in.readNBytes(4);
        System.out.println("Correlation ID: " + ByteBuffer.wrap(correlationId).getInt());

        byte[] body = in.readNBytes((size - 8));
        byte[] response = createResponse(apiVersion, correlationId);

        // Write the response size (32 bits) and the response itself
        byte[] sizeBytes = ByteBuffer.allocate(4).putInt(response.length).array();
        out.write(sizeBytes);
        out.write(response);
        out.flush();
    }

    private static byte[] createResponse(short apiVersion, byte[] correlationId) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            bos.write(correlationId);

            if (apiVersion < 0 || apiVersion > 4) {
                // unsupported API version
                bos.write(new byte[] { 0, 35 });
            } else {
                // Write success response
                bos.write(new byte[] { 0, 0 }); // Error code (16 bits)
                bos.write(2); // Array size (1 + 1)

                // API key information
                bos.write(new byte[] { 0, 18 }); // API key (16 bits)
                bos.write(new byte[] { 0, 3 }); // Min version (16 bits)
                bos.write(new byte[] { 0, 4 }); // Max version (16 bits)
                bos.write(0); // Tagged fields (1 byte)

                // Throttle time
                bos.write(new byte[] { 0, 0, 0, 0 }); // Throttle time (32 bits)
                bos.write(0); // End tagged fields (1 byte)
            }

            System.out.println("Response completed, size: " + bos.size());
            System.out.println();
            return bos.toByteArray();
        } catch (Exception e) {
            System.err.println("Error creating response: " + e.getMessage());
            return new byte[0];
        }
    }
}
