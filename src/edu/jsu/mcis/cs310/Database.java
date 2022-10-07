package edu.jsu.mcis.cs310;

import java.sql.*;
import org.json.simple.*;
import org.json.simple.parser.*;

public class Database {
    
    private final Connection connection;
    
    private final int TERMID_SP22 = 1;
    
    /* CONSTRUCTOR */

    public Database(String username, String password, String address) {
        
        this.connection = openConnection(username, password, address);
        
    }
    
    /* PUBLIC METHODS */

    public String getSectionsAsJSON(int termid, String subjectid, String num) {
        
        String result = null;
        
        
        // INSERT YOUR CODE HERE
       
     try{
         

        String query;
        query = "SELECT * FROM section WHERE termid ="+termid+ " AND subjectid ='"+subjectid+"' AND num ='"+num+"'";
        
        //Prepare query
        PreparedStatement pstmt = null;
        pstmt = connection.prepareStatement(query);
        //Execute query
        boolean hasresults;
        hasresults = pstmt.execute();
        
       
        
        //Check for Results
        if (hasresults) {
        ResultSet resultset;
        //Get Results
        resultset = pstmt.getResultSet();
        result = getResultSetAsJSON(resultset);
        
       }

        else {

            System.err.println("ERROR: No data returned!");

            }
            
     }
     
     catch (Exception e) { e.printStackTrace(); }
       
        return result;
        
    
    }
    
    public int register(int studentid, int termid, int crn) {
        
        int result = 0;
        
        // INSERT YOUR CODE HERE
        String query;
        PreparedStatement pstUpdate = null;
        int updateCount;
        
        try{
           /* Prepare Update Query */
           
            query = "INSERT INTO registration (studentid, termid, crn) VALUES (?, ?, ?)";
            pstUpdate = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            pstUpdate.setInt(1, studentid);
            pstUpdate.setInt(2, termid);
            pstUpdate.setInt(3, crn);
        
        //Execute query
            updateCount = pstUpdate.executeUpdate();
        
         /* If Update Successful, update results */
         
            result =  updateCount;
         
           
        
        }
        
        catch (Exception e) {  e.printStackTrace(); }
        
        return result;
        
    }

    public int drop(int studentid, int termid, int crn) {
        
        int result = 0;
        
        // INSERT YOUR CODE HERE
        int updateCount;
        PreparedStatement pstd = null;
        
        try{
        /* Prepare Update Query */
        
            String query = "DELETE FROM registration WHERE studentid = ? AND termid = ? AND crn = ?";
            pstd = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            pstd.setInt(1, studentid);
            pstd.setInt(2, termid);
            pstd.setInt(3, crn);
        
            //Execute query
            updateCount = pstd.executeUpdate();
         
         
          /* If Update Successful, update result */
                
                if (updateCount > 0) {
            
                  result = updateCount;

                }
         
            
        }
        
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    public int withdraw(int studentid, int termid) {
        
        int result = 0;
        
        // INSERT YOUR CODE HERE
        
        PreparedStatement pstUpdate = null;
        try {
            /* Prepare Update Query */
            String query = "DELETE FROM registration WHERE studentid = ? AND termid = ?";
            pstUpdate = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            pstUpdate.setInt(1, studentid);
            pstUpdate.setInt(2, termid);
            
            //Excecute query
            int updateCount = pstUpdate.executeUpdate();
            
            //Update results
            result = updateCount;
        }
        
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    public String getScheduleAsJSON(int studentid, int termid) {
        
        String result = null;
        
        // INSERT YOUR CODE HERE
        
        try{
            //Prepare Select query
            String query;
            query = "SELECT * FROM registration JOIN section ON registration.crn =section.crn;";
            PreparedStatement pstUpdate = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS); 
            //Execute query
            boolean hasresults = pstUpdate.execute();
            //Check results
            if(hasresults) {
                //Get results
                ResultSet resultset = pstUpdate.getResultSet();
                result = getResultSetAsJSON(resultset);
            }
        }
        
         catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    public int getStudentId(String username) {
        
        int id = 0;
        
        try {
        
            String query = "SELECT * FROM student WHERE username = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, username);
            
            boolean hasresults = pstmt.execute();
            
            if ( hasresults ) {
                
                ResultSet resultset = pstmt.getResultSet();
                
                if (resultset.next())
                    
                    id = resultset.getInt("id");
                
            }
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return id;
        
    }
    
    public boolean isConnected() {

        boolean result = false;
        
        try {
            
            if ( !(connection == null) )
                
                result = !(connection.isClosed());
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    /* PRIVATE METHODS */

    private Connection openConnection(String u, String p, String a) {
        
        Connection c = null;
        
        if (a.equals("") || u.equals("") || p.equals(""))
            
            System.err.println("*** ERROR: MUST SPECIFY ADDRESS/USERNAME/PASSWORD BEFORE OPENING DATABASE CONNECTION ***");
        
        else {
        
            try {

                String url = "jdbc:mysql://" + a + "/jsu_sp22_v1?autoReconnect=true&useSSL=false&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=America/Chicago";
                // System.err.println("Connecting to " + url + " ...");

                c = DriverManager.getConnection(url, u, p);

            }
            catch (Exception e) { e.printStackTrace(); }
        
        }
        
        return c;
        
    }
    
    private String getResultSetAsJSON(ResultSet resultset) {
        
        String result;
        
        /* Create JSON Containers */
        
        JSONArray json = new JSONArray();
        JSONArray keys = new JSONArray();
        
        try {
            
            /* Get Metadata */
        
            ResultSetMetaData metadata = resultset.getMetaData();
            int columnCount = metadata.getColumnCount();
            
            // INSERT YOUR CODE HERE
            

                    String key, value;

                  
                    /* Get Column Names; */

                    for (int i = 1; i <= columnCount; ++i) {

                        key = metadata.getColumnLabel(i);
                        keys.add(key);
                      

                    }

                    /* Get Data; Print as Table Rows */

                    while(resultset.next()) {

                        /* Begin Next ResultSet Row */
                        //JSONObject to connect keys and values
                        JSONObject data = new JSONObject();
                        

                        /* Loop Through ResultSet Columns; Print Values */
                        
                            for (int i = 1; i <= columnCount; ++i) {
                                
                                /* Get Data; */
                                value = resultset.getString(i);
                                //Attach keys to values
                                data.put(keys.get(i-1), value);
                              
                             }
                            //Add row to Array
                            json.add(data);
                        
                
                    }
                   

               
        
        }
        catch (Exception e) { e.printStackTrace(); }
        
        /* Encode JSON Data and Return */
        
        result = JSONValue.toJSONString(json);
        return result;
        
    }
    
}