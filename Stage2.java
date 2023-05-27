//COMP3100 PROJECT STAGE 1 
//WORK BY THOMAS KELLY, 47121696




import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Stage2 {

    
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
            int largestServerCount = 0;
            int nextServerIndex = 0;
            String ServerType = null; 
            String largestServerTypeName = "placeholder";
            int ServerID = 0;
            int lastServerID = -1;
            boolean datadone = false;
            int indexloc = 0; 

            List<String> largestServerTypeNames = new ArrayList<>();
            List<Integer> largestServerTypeCounts = new ArrayList<>();
            


              
           

            out.write("HELO\n".getBytes());
            out.flush();
            System.out.println("SENT: HELO"); //send HELO
            inputLine = in.readLine();
            if (inputLine.equals("OK")) { //receive OK
                System.out.println("RCVD: " + inputLine); //print
                out.write("AUTH tom\n".getBytes());
                out.flush();
                System.out.println("SENT: AUTH");
                inputLine = in.readLine();
                if(inputLine.equals("OK")){ // ok to auth
                    System.out.println("RCVD: " + inputLine); //rcvd: OK
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

            out.write("REDY\n".getBytes()); //REDY
            out.flush();
            System.out.println("SENT: REDY"); //sent redy
            inputLine = in.readLine(); //get jobn
            String firstjob = inputLine;

            System.out.println("RCVD: " + inputLine); //JOBN INFO E.g
            if (inputLine == null) {
                System.out.println("NULL"); // print if null
                out.write("QUIT\n".getBytes());
                out.flush();
                } //end if
            fields = inputLine.split("\\s+");
            String[]typeresponse = fields;

            if (fields[0].equals("NONE")) { //NONE
                System.out.println("SENT: QUIT"); //quit if NONE
                out.write("QUIT\n".getBytes());
                out.flush();
                inputLine = in.readLine();
                System.out.println("RCVD: " + inputLine); 
            }  //end none

            if (!fields[0].equals("NONE") && datadone == false) {
                out.write("GETS All\n".getBytes());
                out.flush();
                System.out.println("SENT: GETS ALL"); //sent GETS ALL
                inputLine = in.readLine(); //data 184 124
                //System.out.println(inputLine); //prints data numbers
                out.write("OK\n".getBytes()); 
                out.flush();
                System.out.println("SENT: OK"); //send OK i want data
               // inputLine = in.readLine();
                
                if(inputLine.startsWith("DATA")) { //reads data 
                    fields = inputLine.split("\\s+");
                    nServers = Integer.parseInt(fields[1]); //e.g data 184 124 = 184
                    // System.out.println("number of servers = " + fields[1]); //print num of servers
                    serverCores = new int[nServers];
                    for (int i = 0; i < nServers; i++) { // for 0 to number of servers (184 FOR DS-SERVER)
                        inputLine = in.readLine(); //input line (first server info)
               //         System.out.println(inputLine);
                        fields = inputLine.split("\\s+");
                        serverCores[i] = Integer.parseInt(fields[4]);
                  
                        if (serverCores[i] > largestServerCores) { //if cores[i] > maxcores
                            largestServerTypeNames.clear(); // clear the arraylist
                            largestServerTypeCounts.clear();
                            largestServerTypeNames.add(fields[0]); //add the serverType
                            largestServerCores = serverCores[i];
                            largestServerTypeCounts.add(1); //add the count = 1
                            largestServerCount = 1; 
                            largestServerType = i;
                      } else if (serverCores[i] == largestServerCores) {
                            int index = largestServerTypeNames.indexOf(fields[0]);  
                            if (index != -1) {
                                int count = largestServerTypeCounts.get(index);
                                largestServerTypeCounts.set(index, count + 1);
                            } else {
                                largestServerTypeNames.add(fields[0]);
                                largestServerTypeCounts.add(1);
                            }
                                 
                        }
                        

                        //else if(serverCores[i] == largestServerCores && (large))
                    
                        } //end if
                    } //end for
                
               
                    
                    out.write("OK\n".getBytes());  //ok ive read data (step 10 done)
                    out.flush();
                    System.out.println("SENT: OK"); //send OK

                    datadone = true; 

                    
                } //end data
              
            //end while loop


            
            inputLine = in.readLine(); //dot
            System.out.println(inputLine); //dot 
           
            

            //while (true) {
                
                //System.out.println("RCVD: " + inputLine); //print DOT

                if(inputLine.equals(".")) { // check if message is DOT

                    System.out.println("yep");

                    
                    
                    if(firstjob == "NONE"){ //IF NONE...
                        System.out.println("SENT: QUIT"); //quit if NONE
                        out.write("QUIT\n".getBytes());
                        out.flush();
                        inputLine = in.readLine();
                        System.out.println("RCVD: " + inputLine);
                       // break;
                    }

                    // } //end if(none)
                    if(firstjob.startsWith("JCPL")){
                        out.write("REDY\n".getBytes());
                        out.flush();
                        System.out.println("SENT: REDY");
                        inputLine = in.readLine();
                        System.out.println("RCVD: " + inputLine);
                        
                    }
                    fields = firstjob.split("\\s+");
                    int jobId = Integer.parseInt(fields[2]); //JOB ID
                    int jobTime = Integer.parseInt(fields[3]); //JOB TIME
                    int jobCores = Integer.parseInt(fields[4]); //JOB CORES
                    int jobMem = Integer.parseInt(fields[5]); //JOB MEMORY
         //           System.out.println("Job information gathered"); //GOT THEM ALL 

                      
                    
           //         
                    
                   

                    
                        String SCHDlargeServerType = largestServerTypeNames.get(indexloc);
                        int SCHDserverCount = largestServerTypeCounts.get(indexloc);
                        ServerID = lastServerID +1;  //starts as -1 so now 0 

                        if(ServerID == SCHDserverCount){
                            ServerID = 0; 
                            indexloc = indexloc + 1; 
                            SCHDlargeServerType = largestServerTypeNames.get(indexloc);
                            SCHDserverCount = largestServerTypeCounts.get(indexloc);
                        } //serverid if
                       
                        lastServerID = ServerID;
                    
                        
                    
                            
                    
                    System.out.println(("SCHD " + jobId + " " + SCHDlargeServerType+ " " + ServerID + "\n"));

                    out.write(("SCHD " + jobId + " " + SCHDlargeServerType+ " " + ServerID + "\n").getBytes());
                  

                    out.flush();
                    nextServerIndex = (nextServerIndex + 1) % largestServerCount; 
               //     System.out.println("Job Successful\n");
                    inputLine = in.readLine(); // ok probably
                    
                    System.out.println("RCVD: " + inputLine);
                    out.write("REDY\n".getBytes());
                    out.flush();
                    System.out.println("SENT: REDY");
                    inputLine = in.readLine();
                    System.out.println("RCVD: " + inputLine);
                    fields = inputLine.split("\\s+");
                }
                    


                    while(!inputLine.equals("NONE")){ //IF JOBN...

                        if(inputLine.startsWith("JCPL")){
                            out.write("REDY\n".getBytes());
                            out.flush();
                            System.out.println("SENT: REDY");
                            inputLine = in.readLine();
                            System.out.println("RCVD: " + inputLine);
                            continue; //back to top
                        } //end of if jcpl
                        fields = inputLine.split("\\s+");
                        int jobId = Integer.parseInt(fields[2]); //JOB ID
                        int jobTime = Integer.parseInt(fields[3]); //JOB TIME
                        int jobCores = Integer.parseInt(fields[4]); //JOB CORES
                        int jobMem = Integer.parseInt(fields[5]); //JOB MEMORY
             //           System.out.println("Job information gathered"); //GOT THEM ALL 
    
                        

                        String SCHDlargeServerType = largestServerTypeNames.get(indexloc);
                        int SCHDserverCount = largestServerTypeCounts.get(indexloc);
                        ServerID = lastServerID +1;  //starts as -1 so now 0 

                        if(ServerID == SCHDserverCount){
                            ServerID = 0; 
                            indexloc = indexloc + 1; 
                            SCHDlargeServerType = largestServerTypeNames.get(indexloc);
                            SCHDserverCount = largestServerTypeCounts.get(indexloc);
                        } //serverid if
                       
                        lastServerID = ServerID;
    
                                
                                
                 //       System.out.println("nServers: " + nServers + " serverCores: " + serverCores + " jobCores: " + jobCores + " nextServerIndex: "+ nextServerIndex);
                        System.out.println(("SCHD " + jobId + " " + SCHDlargeServerType+ " " + ServerID + "\n"));

                        out.write(("SCHD " + jobId + " " + SCHDlargeServerType+ " " + ServerID + "\n").getBytes());
                        
    
                        out.flush();
                        nextServerIndex = (nextServerIndex + 1) % largestServerCount; 
                   //     System.out.println("Job Successful\n");
                        inputLine = in.readLine(); 
                        
                        System.out.println("RCVD: " + inputLine);
                        out.write("REDY\n".getBytes());
                        out.flush();
                        System.out.println("SENT: REDY");
                        inputLine = in.readLine();
                        System.out.println("RCVD: " + inputLine);
                       // continue; // go to next iteration of the loop

                    } //end while jobs != none
                    
                    
                   
       
                
                
              //  System.out.println("RCVD2: " + inputLine); //JOBN INFO E.g
                if (inputLine == null) {
                    System.out.println("NULL"); // print if null
                  //  break;
                } //end if
                fields = inputLine.split("\\s+");
                typeresponse = fields;
                
             
                if (fields[0].equals("NONE")) { //NONE
                    System.out.println("SENT: QUIT"); //quit if NONE
                    out.write("QUIT\n".getBytes());
                    out.flush();
                    inputLine = in.readLine();
                    System.out.println("RCVD: " + inputLine); 
                    //break;
                }  //end none
            

        
        } //try }
    
            catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            
            }
        }
    
    
        
}