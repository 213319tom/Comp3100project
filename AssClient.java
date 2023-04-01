//COMP3100 PROJECT STAGE 1 
//WORK BY THOMAS KELLY, 47121696




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
            int largestServerCount = 0;
            int nextServerIndex = 0;
            String ServerType = null; 
            String largestServerTypeName = "placeholder";
            int ServerID = 0;
            int lastServerID = -1;
            boolean datadone = false;
            


              
           

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
                        
                            largestServerTypeName = fields[0];
                            largestServerCores = serverCores[i];
                            largestServerCount = 1; 
                            largestServerType = i;
                      } else if (serverCores[i] == largestServerCores && (largestServerTypeName.equals(fields[0])) ) {
                                largestServerCount++;
                 //               System.out.println("Largest server count: " + largestServerCount);
                        }
                    
                        } //end if
                    } //end for
                
                //    System.out.println("LargestServerTypeName = " + largestServerTypeName);
                 //   System.out.println("LargestServerCores = " + largestServerCores); // print the 3 largest server details
                  //  System.out.println("LargestServerCount = " + largestServerCount);
                    
                    out.write("OK\n".getBytes());  //ok ive read data (step 10 done)
                    out.flush();
                    System.out.println("SENT: OK"); //send OK


                    //in.readLine();
                    // System.out.println(fields[0]); //print 4xlarge for example (server) 
                    // System.out.println("ID = " + typeresponse[2]); // print jobn for example
                    // System.out.println(typeresponse[0]);
                    datadone = true; 

                    // out.write("REDY\n".getBytes());
                    // out.flush();
                    // System.out.println("SENT: REDY"); //sent redy
                    // inputLine = in.readLine(); //servers response
                } //end data
              
            //end while loop


            
            inputLine = in.readLine(); //dot
            System.out.println(inputLine); //dot 
           
            

            //while (true) {
                
                //System.out.println("RCVD: " + inputLine); //print DOT

                if(inputLine.equals(".")) { // check if message is DOT

                    System.out.println("yep");

                    // out.write("REDY\n".getBytes());
                    // out.flush();
                    // System.out.println("SENT: REDY"); //sent redy
                    
                    // inputLine = in.readLine(); //JOBN X Y Z or NONE OR JCPL
                    // System.out.println(inputLine);
                    
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

                      
                    
           //         System.out.println("LST = " + largestServerType + " LSCores = " + largestServerCores + " LSCount = " + largestServerCount);
             
                         
                    //fields = lastServer.split("\\s+"); //fields = last server as array
                    //System.out.println("lastserver: " + lastServer);  //prints last server 
                    //System.out.println(fields[1]); //prints last server id
                   // ServerID = Integer.parseInt(fields[1]) + 1; //server id is equal to last server id
                    
                   ServerID = lastServerID +1; 

                    if(ServerID == largestServerCount){
                        ServerID = 0; 
                    }
                   
                    lastServerID = ServerID;

                                      
                            //    }
        
                        //0 = -1 + 1 = 0, 0 <3, lsid = 0
                        //sid = 0 + 1 = 1, 1 < 3, lsid = 1
                        //sid = 1 + 1 = 2, 2 <3, lsid = 2
                        //sid = 2 + 1 = 3, 3 ==3, sid = 0, lsid = 0
                        //sid = 0 + 1= 1  1 <3, lsid = 1
                     
                    //}
                        

                    
                    
                            
             //       System.out.println("nServers: " + nServers + " serverCores: " + serverCores + " jobCores: " + jobCores + " nextServerIndex: "+ nextServerIndex);
                    System.out.println(("SCHD " + jobId + " " + largestServerTypeName+ " " + ServerID + "\n"));

                    out.write(("SCHD " + jobId + " " + largestServerTypeName+ " " + ServerID + "\n").getBytes());

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
    
                          
                        
               //         System.out.println("LST = " + largestServerType + " LSCores = " + largestServerCores + " LSCount = " + largestServerCount);
                 
                             
                        //fields = lastServer.split("\\s+"); //fields = last server as array
                        //System.out.println("lastserver: " + lastServer);  //prints last server 
                        //System.out.println(fields[1]); //prints last server id
                       // ServerID = Integer.parseInt(fields[1]) + 1; //server id is equal to last server id
                        
                       ServerID = lastServerID +1; 

                        if(ServerID == largestServerCount){
                            ServerID = 0; 
                        } //serverid if
                       
                        lastServerID = ServerID;
    
                                          
                                //    }
            
                            //0 = -1 + 1 = 0, 0 <3, lsid = 0
                            //sid = 0 + 1 = 1, 1 < 3, lsid = 1
                            //sid = 1 + 1 = 2, 2 <3, lsid = 2
                            //sid = 2 + 1 = 3, 3 ==3, sid = 0, lsid = 0
                            //sid = 0 + 1= 1  1 <3, lsid = 1
                         
                        //}
                            
    
                        
                        
                                
                 //       System.out.println("nServers: " + nServers + " serverCores: " + serverCores + " jobCores: " + jobCores + " nextServerIndex: "+ nextServerIndex);
                        System.out.println(("SCHD " + jobId + " " + largestServerTypeName+ " " + ServerID + "\n"));

                        out.write(("SCHD " + jobId + " " + largestServerTypeName+ " " + ServerID + "\n").getBytes());
    
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
                    
                    
                   
       // } //what is this
                
                
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