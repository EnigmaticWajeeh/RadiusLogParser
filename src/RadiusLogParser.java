import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/* copyright wajeeh */

public class RadiusLogParser {

	

	public static void main(String[] args) {

		boolean loginOK;
		boolean loginIncorrect;
		String macAddress;
		String userName;
		String date;
		String month;
		String year;
		String time;
		String day;
		String type;
		Integer loginAttempts;
		String infoDesc;
		boolean error;
		
		
		
		
		
		
		// Initial Setup for parser
		
		ArrayList<String> inputArguments=new ArrayList<String>();
		
		
		if(args.length<4){
			System.out.println("make sure arguments are <radius log filename>" + 
		" <seed ip address> <keyspace name> <tablename>");
			System.exit(1);
		} else{
			
			// file name
			inputArguments.add(args[0]);
			
			//ip address
			inputArguments.add(args[1]);

			//keyspace name
			inputArguments.add(args[2]);

			//table name
			inputArguments.add(args[3]);
		}
		
		System.out.println("Parsing the file given in first argument");
		
		BufferedReader br=null;
		String currentLine;
		
		File firstFile=new File(inputArguments.get(0));
		
		// intial setup for parser done, read first argument file and start parsing
		
		
		
		
		
		
//		CONNECTION PROPERTIES FOR APACHE CASSANDRA
		
//			String serverIp = "127.0.0.1";
//
//		    String keyspace = "log";
//		    String tableName="radius";
		    
			String serverIp = inputArguments.get(1);

		    String keyspace = inputArguments.get(2);
		    String tableName= inputArguments.get(3);
		    
		    		    
		    Cluster cluster = Cluster.builder()
		            .addContactPoints(serverIp)
		            .build();

		    String createKeyspaceString="CREATE KEYSPACE IF NOT EXISTS " + keyspace + " " +
		    "with " + "replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}";
		    
		    String createTableString= "CREATE TABLE IF NOT EXISTS "+ tableName +
		    "( event_id timeuuid primary key,"+
		    " loginok boolean, " +
		    "loginIncorrect boolean, " +  
		    "error boolean, " +
		    "type text, " + 
		    "macAddress text, " +
		    "userName text, " +
		    "year text, " +
		    "month text, " +
		    "date text," +
		    "time text, " + 
		    "day text, " +
		    "loginAttempts int," +
		    "description text, " +
		    ")" ;
		    
		    		/*
		    		 * insert into radius (loginok, loginIncorrect, macAddress,
		    		 * userName, date, month, year, time, day, type, loginAttempts,
		    		 *  description, error) values (now(),);
		    		 */
		    
		    // CONNECTION SETTINGS ENDED
		    
		    
		    
		    
		    
		    // CREATING SESSION, KEYSPACE AND TABLE
		    
//		    Session session = cluster.connect(keyspace);
		    Session session = cluster.connect();
		    
		    session.execute(createKeyspaceString);
		    
		    session.execute("use " + keyspace);
		    
//		    System.out.println(createTableString);
		    
		    session.execute(createTableString);
		    
		    // DONE CREATING SESSION< KEYSPACE AND TABLE
		    
		    
		    
		    
		    
		//File exists, now start reading from the file
		
		if(firstFile.exists()){
			System.out.println("The first file exists!");
			
			try {
				
				br=new BufferedReader(new FileReader(inputArguments.get(0)));
				
				// Reading from the lines
				
				error = false;
				
				PrintWriter pr=new PrintWriter("op.txt","UTF-8");
				
				while((currentLine = br.readLine()) != null){
					
//					System.out.println(currentLine);
					
					// RESET THE VALUES
					loginOK=false;
					loginIncorrect=false;
					
					// TODO check MacAddress And userName
					macAddress=null;
					userName=null;
					error=false;
					
					infoDesc=null;
					date=null;
					month=null;
					year=null;
					time=null;
					day=null;
					type=null;
					loginAttempts=null;
					
					month=currentLine.substring(4,7);
					// month done
					date=currentLine.substring(8,10);
					// date done
					year=currentLine.substring(20,24);
					// year done
					
					
					int endOfDate=currentLine.indexOf(" : Info");
					
					if(endOfDate<0){
						endOfDate=currentLine.indexOf(" : Auth");
						
						if(endOfDate<0){
							endOfDate=currentLine.indexOf(" : Error");
							error=true;
							
						}
						
					} 
					
					
					type=currentLine.substring(endOfDate+3,endOfDate+7);
					
					if(error){
						
						type=currentLine.substring(endOfDate+3,endOfDate+8);
						
					}
					// type done
					
					// now parse differently according to different TYPES

					
					time=(String) currentLine.subSequence(11, 19);					
					// time done
					
					
					day=currentLine.substring(0, 3);
					
					
					// parsed till date and time, also the type is now known
					int prevTotalLength=currentLine.length();
					
					
					String newCurrentLine=currentLine.substring(endOfDate + 8,prevTotalLength);
					if(error){
						newCurrentLine=currentLine.substring(endOfDate + 10,prevTotalLength);
						infoDesc=newCurrentLine;
					}
					
					
					// NOW WE HAVE NEW STRING
					
					if(type.equals("Info")){
						
						error=false;
					
						infoDesc=newCurrentLine.substring(1, newCurrentLine.length());
					
						// Info part done
					
					}else if(type.equals("Auth")){
					// Analyze Auth and parse everything
						
						error=false;
						newCurrentLine=newCurrentLine.substring(1,newCurrentLine.length());
						
						List<String> myAuthList;
						myAuthList=Arrays.asList(newCurrentLine.split(":"));
						
						
						loginOK=false;
						loginIncorrect=false;
						
						
						if(myAuthList.get(0).equals("Login OK")){
//							System.out.println(myAuthList.get(1));
							// tested : receiving results of only Login OK
							
							
							loginOK=true;
							//algo for finding username
							int userNameBegins=myAuthList.get(1).indexOf('[');
							int userNameEnds=myAuthList.get(1).indexOf(']');
							
							userName= myAuthList.get(1).substring(userNameBegins + 1, userNameEnds);
							// tested : userName done
							
//							System.out.println(userName);
							
							int lastBracketC=myAuthList.get(1).lastIndexOf(')');

							
							macAddress= myAuthList.get(1).substring(lastBracketC-17, lastBracketC);
							
						
						}else if(myAuthList.get(0).equals("Login incorrect")){
//							System.out.println(myAuthList.get(1));
							// tested : receiving results of only Login incorrect
							
							loginIncorrect=true;
							
							int userNameBegins=myAuthList.get(1).indexOf('[');
							int userNameEnds=myAuthList.get(1).indexOf("/");
							
							
							// ADDING EXCLUSION
							if((userNameBegins > 0) && (userNameEnds > 0)){
							
							userName= myAuthList.get(1).substring(userNameBegins + 1, userNameEnds);
								
							
							int lastBracketC=myAuthList.get(1).lastIndexOf(')');
							
							macAddress= myAuthList.get(1).substring(lastBracketC-17, lastBracketC);
							}
							
							
							
						}else {
							
//							System.out.println(myAuthList.get(1));
//							 tested getting multiple logins only
							
							// ADD EXCLUSION
							
							type = "Multiple logins";
							
							
							if((myAuthList.get(0).substring(21, 22).equals("0"))
							||(myAuthList.get(0).substring(21, 22).equals("1"))
							||(myAuthList.get(0).substring(21, 22).equals("2"))
							||(myAuthList.get(0).substring(21, 22).equals("3"))
							||(myAuthList.get(0).substring(21, 22).equals("4"))
							||(myAuthList.get(0).substring(21, 22).equals("5"))
							||(myAuthList.get(0).substring(21, 22).equals("6"))
							||(myAuthList.get(0).substring(21, 22).equals("7"))
							||(myAuthList.get(0).substring(21, 22).equals("8"))
							||(myAuthList.get(0).substring(21, 22).equals("9"))){
							
							loginAttempts= Integer.valueOf(myAuthList.get(0).substring(21, 22)); 
							
							}
							
							
							if(myAuthList.get(1).contains("[")){

								int userNameBegins=myAuthList.get(1).indexOf('[');
								int userNameEnds=myAuthList.get(1).indexOf(']');
								userName=myAuthList.get(1).substring(userNameBegins + 1, userNameEnds);
								// DOne : userName Obtained
								int lastBracketC=myAuthList.get(1).lastIndexOf(')');
								
								macAddress= myAuthList.get(1).substring(lastBracketC-17, lastBracketC);
							}
							

														
						}
						
						
						
						
						
						
						
					} else if(type.equals("error")){
//						System.out.println("Error found");
					}
					
					// TODO Complete this
					
					System.out.println(" type : " + type + " MacAddress : " + macAddress +
							" userName : " + " " + userName + " " + date + " " +
							month + " " + year + " " + time + " " + day + 
							" loginOK : " + loginOK + " loginIncorrect : " + 
							loginIncorrect +
							" error : "	+ error  + " Multiple Login Attempts : " +
							loginAttempts + " Desc : " + infoDesc);
//					System.out.println("InfoDesc : " + infoDesc);
					
					
					// INSERT INTO TABLE
					
					try{
					if(infoDesc.contains("'")){
//						System.out.println("info : " + infoDesc);
//						infoDesc.replace("'", "");
						
						infoDesc=infoDesc.replace("'", "");
						
						System.out.println("info : " + infoDesc);
						
					}}catch(NullPointerException x){
						System.out.println("error here : " + infoDesc);
					}
					
					// TODO insert query string
					String insertQueryString = "INSERT INTO radius (event_id, date, day, description, " +
				    		"error, loginattempts, loginincorrect, loginok, macaddress, month, time," +
				    		" type, username, year) values "+"(now(), " +"'" + date +"'" + ", " +"'" + day + "'" 
				    		+ ", " + "'" + infoDesc 
				    		+ "'" + ", " + error + ", " + loginAttempts + ", " + loginIncorrect
				    		+ ", " + loginOK + ", "+"'" + macAddress +"'" + ", " +"'" + month +"'"  +
				    		", " +"'"  + time +"'"  + ", " +"'"  + type +"'"  + ", " + "'"  + userName +"'" 
				    		+ ", " +"'"   + year +"'"  + ")";
					    
						System.out.println(insertQueryString);
						System.out.println(infoDesc);
						
					    System.out.println(" " + loginOK + loginIncorrect + macAddress + userName +
					    		date + month + year + time + day + type + loginAttempts + infoDesc +
					    		error );
					    
					    System.out.println(insertQueryString);
					    
					    session.execute(insertQueryString);
					
					    System.out.println("Query executed");
					// INSERT INTO TABLE DONE
					
					
					pr.println(" type : " + type + " MacAddress : " + macAddress +
							" userName : " + " " + userName + " " + date + " " +
							month + " " + year + " " + time + " " + day + 
							" loginOK : " + loginOK + " loginIncorrect : " +
							loginIncorrect +
							" error : "	+ error  + " Multiple Login Attempts : " +
							loginAttempts + " Desc : " + infoDesc);
					
					
				}	// while loop ends here
				pr.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				
				try {
					if (br != null)br.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
			
			
		
		}else{
			System.out.println("The first file does not exists!");
		}
		
		 String cqlStatement = "SELECT * FROM " + tableName;
			
		    for (Row row : session.execute(cqlStatement)) {	    
		        System.out.println(row.toString());
		    }

	} // main function ends here

}
