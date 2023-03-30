import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class AssClient {

    public static void main(String[] args) {
        try {
            Socket socket = new Socket("127.0.0.1", 50000);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            String inputLine;
            String[] fields;
            int nServers = 0;
            int[] serverCores = null;
            int largestServerType = -1;
            int largestServerCores = -1;
            int largestServerCount = -1;
            int nextServerIndex = 0;


              
           //

            out.write("HELO\n".getBytes());
            out.flush();
            System.out.println("SENT: HELO"); //send HELO
            inputLine = in.readLine();
            if (inputLine.equals("OK")) { //receive OK
                System.out.println("RCVD: " + inputLine); //print
                out.write("AUTH 47121696\n".getBytes());
                out.flush();
                System.out.println("SENT: AUTH");
                inputLine = in.readLine();
                if(inputLine.equals("OK")){
                    System.out.println("RCVD: " + inputLine);
                }
                
                if (!inputLine.equals("OK")) {
                    System.err.println("Authentication failed: " + inputLine);
                    System.out.println("Authentication failed: " + inputLine);
                    return;
                }
                
            } else {
                System.err.println("Handshake failed: " + inputLine);
                System.out.println("Handshake failed: " + inputLine);
                return;
            }


            

           
            

            while (true) {
                out.write("REDY\n".getBytes());
                out.flush();
                System.out.println("SENT: REDY"); //sent redy
                inputLine = in.readLine();
                System.out.println(inputLine); //JOBN INFO E.g
                if (inputLine == null) {
                    System.out.println("test: 2");
                    break;
                }
                fields = inputLine.split("\\s+");
                
             
                if (fields[0].equals("NONE")) { //NONE
                    System.out.println("QUIT");
                    out.write("QUIT\n".getBytes());
                    out.flush();
                    in.readLine();
                    break;
                } else if (fields[0].equals("JOBN")) {
                    out.write("GETS All\n".getBytes());
                    out.flush();
                    System.out.println("SENT: GETS ALL");
                    inputLine = in.readLine(); //data 184 124
                    System.out.println(inputLine);
                    out.write("OK\n".getBytes()); 
                    out.flush();
                    inputLine = in.readLine();
                    System.out.println(inputLine);
                    out.write("OK\n".getBytes());
                    out.flush();
                    System.out.println("SENT: OK");
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
                        out.write("OK\n".getBytes());
                        out.flush();
                        in.readLine();
                    }
                    out.write("REDY\n".getBytes());
                    out.flush();
               } else if (fields[0].equals("JOBN")) {
                    System.out.println("test: 3.1");
                    int jobId = Integer.parseInt(fields[2]);
                    System.out.println("test: 3.2");
                    int jobTime = Integer.parseInt(fields[3]);
                    System.out.println("test: 3.3");
                    int jobCores = Integer.parseInt(fields[4]);
                    System.out.println("test: 3.4");
                    int jobMem = Integer.parseInt(fields[5]);
                    System.out.println("test: 3.5");
                    out.write(("SCHD " + jobId + " " + getServerType(nServers, serverCores, jobCores, nextServerIndex) + " 0\n").getBytes());
                    System.out.println("test: 3.6");
                    out.flush();
                    System.out.println("test: 69");
                    nextServerIndex = (nextServerIndex + 1) % largestServerCount;
                    System.out.println("test: 5");
                }
            }
            


        } //try }
    
            catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                
            }
        }
        private static int getServerType(int nServers, int[] serverCores, int jobCores, int startIndex) {
            int largestServerType = 0;
            int largestServerCores = serverCores[0];
            int largestServerCount = 1;
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
        }
}
