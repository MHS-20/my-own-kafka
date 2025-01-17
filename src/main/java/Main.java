import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        System.err.println("Starting server...");
        final int port = 9092;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            try (Socket clientSocket = serverSocket.accept();
                 InputStream in = clientSocket.getInputStream();
                 OutputStream out = clientSocket.getOutputStream()) {

                processClientRequest(in, out);

            } catch (IOException e) {
                System.err.println("Client communication error: " + e.getMessage());
            }

        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    private static void processClientRequest(InputStream in, OutputStream out) throws IOException {
        // Read and ignore the first 4 bytes (request size)
        in.readNBytes(4);

        // Read the API key (16 bits) and API version (16 bits)
        byte[] apiKey = in.readNBytes(2);
        short apiVersion = ByteBuffer.wrap(in.readNBytes(2)).getShort();

        // Read the correlation ID (32 bits)
        byte[] correlationId = in.readNBytes(4);

        // Create the response
        byte[] response = createResponse(apiVersion, correlationId);

        // Write the response size (32 bits) and the response itself
        byte[] sizeBytes = ByteBuffer.allocate(4).putInt(response.length).array();
        out.write(sizeBytes);
        out.write(response);
        out.flush();
    }

    private static byte[] createResponse(short apiVersion, byte[] correlationId) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            // Write the correlation ID
            bos.write(correlationId);

            if (apiVersion < 0 || apiVersion > 4) {
                // Write error code (16 bits) for unsupported API version
                bos.write(new byte[]{0, 35});
            } else {
                // Write success response
                bos.write(new byte[]{0, 0}); // Error code (16 bits)
                bos.write(2);                // Array size (1 + 1)

                // API key information
                bos.write(new byte[]{0, 18}); // API key (16 bits)
                bos.write(new byte[]{0, 3});  // Min version (16 bits)
                bos.write(new byte[]{0, 4});  // Max version (16 bits)

                bos.write(0);                // Tagged fields (1 byte)

                // Throttle time
                bos.write(new byte[]{0, 0, 0, 0}); // Throttle time (32 bits)
                bos.write(0);                // End tagged fields (1 byte)
            }

            return bos.toByteArray();
        }
    }
}
