import java.io.*;
import java.net.*;

public class AssClient {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 50000;

    public static void main(String[] args) {
        try (Socket socket = new Socket(HOST, PORT);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String inputLine;
            String[] fields;
            int nServers = 0;
            int[] serverCores = null;
            int largestServerType = -1;
            int largestServerCores = -1;
            int largestServerCount = -1;
            int nextServerIndex = 0;

            out.println("HELO");
            inputLine = in.readLine();
            if (inputLine.equals("OK")) {
                out.println("AUTH random");
                inputLine = in.readLine();
                if (!inputLine.equals("OK")) {
                    System.err.println("Authentication failed: " + inputLine);
                    return;
                }
            } else {
                System.err.println("Handshake failed: " + inputLine);
                return;
            }

            while ((inputLine = in.readLine()) != null) {
                fields = inputLine.split("\\s+");
                if (fields[0].equals("NONE")) {
                    out.println("QUIT");
                    in.readLine();
                    break;
                } else if (fields[0].equals("REDY")) {
                    out.println("GETS All");
                    inputLine = in.readLine();
                    if (inputLine.startsWith("DATA")) {
                        fields = inputLine.split("\\s+");
                        nServers = Integer.parseInt(fields[1]);
                        serverCores = new int[nServers];
                        for (int i = 0; i < nServers; i++) {
                            inputLine = in.readLine();
                            fields = inputLine.split("\\s+");
                            serverCores[i] = Integer.parseInt(fields[4]);
                            if (serverCores[i] > largestServerCores || (serverCores[i] == largestServerCores && largestServerCount < 1)) {
                                largestServerType = i;
                                largestServerCores = serverCores[i];
                                largestServerCount = Integer.parseInt(fields[3]);
                            }
                        }
                        out.println("OK");
                        in.readLine();
                    }
                    out.println("REDY");
                } else if (fields[0].equals("JOBN")) {
                    int jobId = Integer.parseInt(fields[2]);
                    int jobTime = Integer.parseInt(fields[3]);
                    int jobCores = Integer.parseInt(fields[4]);
                    int jobMem = Integer.parseInt(fields[5]);
                    out.println("SCHD " + jobId + " " + getServerType(nServers, serverCores, jobCores, nextServerIndex) + " 0");
                    nextServerIndex = (nextServerIndex + 1) % largestServerCount;
                }
            }

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private static int getServerType(int nServers, int[] serverCores, int jobCores, int startIndex) {
        int largestServerType = -1;
        int largestServerCores = -1;
        int largestServerCount = -1;
        int nextServerIndex = startIndex;

       for(int i = 0; i < nServers; i++) {
            int serverIndex = (nextServerIndex + i) % nServers;
            if (serverCores[serverIndex] >= jobCores) {
                if (largestServerType == -1 || serverCores[serverIndex] < largestServerCores || (serverCores[serverIndex] == largestServerCores && largestServerCount > 1)) {
                    largestServerType = serverIndex;
                    largestServerCores = serverCores[serverIndex];
                    largestServerCount = 1;
                }
            }
        }
        return largestServerType;
    }}
