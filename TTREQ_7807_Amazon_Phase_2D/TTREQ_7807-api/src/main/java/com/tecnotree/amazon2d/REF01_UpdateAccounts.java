package com.tecnotree.amazon2d;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;

import com.jcraft.jsch.JSchException;
import com.tecnotree.tools.Tn3Logger;
import com.tecnotree.tools.Tn3MySQL;

/**
 * Created by: Ing. Franklin Cevallos Gaibor
 * Creation date: 29-06-2021
 *
 * Class updates the promoID column in the Accounts table
 * 
 * Input parameters: Date from
 */

/**
 * @author cevalfr
 *
 */
/**
 * @author cevalfr
 *
 */
public class REF01_UpdateAccounts{
	
	//CONSTANTES
	private static String PROP_FILE_NAME = "configurationUpdateAccounts.properties";
		
	//VARIABLES
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static Tn3MySQL tMySql = null;
	private static Tn3Logger logger = null;
	private static String db_ipServer = "", db_portServer = "", db_user = "", db_password = "", db_schema = "";
	private static String ssh_host = "", ssh_port = "", ssh_user = "", ssh_password = "";
	private static String log_outputDirectory = "";
			
	private static String pattern1="^(\\d{4})(\\/|-)(0[1-9]|1[0-2])\\2([0-2][0-9]|3[0-1])$";
	private static String withSsh = "";
	
	private static Connection conn = null;
	
    public static void main( String[] args ) throws ClassNotFoundException, SQLException, JSchException, InstantiationException, IllegalAccessException
    {
        
    	//OBTENGO LOS PARAMETROS
  		if(args.length == 0) {//SI NO SE PASARON PARAMETROS
  			System.out.println(dateFormat.format(new Date()) + " - No se envio ningun parametro.");
  			
  		}else {
  			
  			//Input parameters
  			String dateCreatedFrom = args[0];
  			if(args.length == 2)//SI SE ENVIA PARAMETRO SSH
  				withSsh = args[1];
  			
  			//VALIDO QUE LA FECHA SEA DEL FORMATO CORRECTO
	  		if(!Pattern.matches(pattern1, dateCreatedFrom)) {
	  			System.out.println(dateFormat.format(new Date()) + " - The parameter DATE don't have the correct format. The formats must be: yyyy-MM-dd.");
	  			System.out.println(dateFormat.format(new Date()) + " - Finished UpdateAccounts.");
	  			return;
	  		}
	  		
	  		//VALIDO QUE EXISTA UNA OPCION DE FECHA
	  		if(Pattern.matches(pattern1, dateCreatedFrom)) {
	  			
	  			//VALIDO SI LA FECHA ES VALIDA
	  			DateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd");
	  			formatoFecha.setLenient(false);
			  		
	  			try {
	  				formatoFecha.parse(dateCreatedFrom);
	  			} catch (ParseException e) {
	  				System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
	  				System.out.println(dateFormat.format(new Date()) + " - Finished UpdateAccounts.");
	  				return;
	  			}
			
	  		}//FIN DE if(Pattern.matches(pattern1, date)) {
	  		
	  		//VALIDO QUE EL PARAMETRO SSH SEA N O S
	  		if(!withSsh.equals(""))
		  		if(!withSsh.equalsIgnoreCase("N") && !withSsh.equalsIgnoreCase("Y")) {
			  		System.out.println(dateFormat.format(new Date()) + " - The parameter SSH don't have the correct format. The formats must be: N or Y.");
			  		System.out.println(dateFormat.format(new Date()) + " - Finished UpdateAccounts.");
			  		return;
		  		}
	  				  		
			try {
				//CARGO EL ARCHIVO DE PROPIEDADES
		  		loadProperties(PROP_FILE_NAME);
		  		
			}catch (IOException|SecurityException|NullPointerException|NumberFormatException e) {
  		  		System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
  		  		System.out.println(dateFormat.format(new Date()) + " - Finished UpdateAccounts.");
  		  		return;
  		  	}
	
			try {
  				
		  		//CREO EL DIRECTORIO Y EL ARCHIVO DE LOGS
		  		logger = new Tn3Logger("UpdateAccounts", log_outputDirectory);
		  		
		  		logger.info("Command executed: UpdateAccounts "+ args[0]);
		  		System.out.println(dateFormat.format(new Date()) + " - Command executed: UpdateAccounts "+ args[0]);
		  						  		
		  		logger.info("Properties file load.");
		  		System.out.println(dateFormat.format(new Date()) + " - Starting update accounts ...");
		  		logger.info("Starting updates accounts...");
		  		
		  		//ACTUALIZO LOS REGISTROS DESDE LA FECHA DE CREACION DE LA CUENTA
		  		updateAccounts(dateCreatedFrom);
		  		
		  		System.out.println(dateFormat.format(new Date()) + " - Finished update accounts.");
  		  		logger.info("Finished update accounts.");
  		  		  			
  			}catch (SecurityException|SQLException|ClassNotFoundException|NullPointerException|NumberFormatException|JSchException e) {
  		  		System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
  		  		logger.severe(e);
		  		
  		  		if(conn != null) {
	  		  		//CIERRO CONEXION CON LA BASE DE DATOS
	  				tMySql.finalizarConexion();
	  				System.out.println(dateFormat.format(new Date()) + " - Connection to database closed.");
	  				logger.info("Connection to database closed.");	
  		  		}
  		  		
  		  		if(withSsh.equalsIgnoreCase("Y")) {
  		  			System.out.println(dateFormat.format(new Date()) + " - SSH tunnel toward database closed.");
  		  			logger.info("SSH tunnel toward database  closed.");	
  		  			tMySql.closeSshTunnel();
				}
  		  		
  		  		System.out.println(dateFormat.format(new Date()) + " - Finished update accounts.");
  		  		logger.info("Finished update accounts.");
  		  		//e.printStackTrace();
  		  	}
    
  		}
        
    }
    
	/**
	 * Method loads the properties.
	 * 
	 * @param propFileName
	 * @throws IOException
	 */
	public static void loadProperties(String propFileName) throws IOException {
		
  		Properties prop = new Properties();
		//InputStream inputStream = new FileInputStream(propFileName);
  		InputStream inputStream = ClassLoader.getSystemResourceAsStream(propFileName);
  		prop.load(inputStream);
		  			
  		//PROPIEDADES DE LA BASE DE DATOS
  		db_ipServer = prop.getProperty("db.ipServer");
  		db_portServer = prop.getProperty("db.portServer");
  		db_user = prop.getProperty("db.user");
  		db_password = prop.getProperty("db.password");
  		db_schema = prop.getProperty("db.schema");
  		
  		//PROPIEDADES DEL LOS LOGS
  		log_outputDirectory = prop.getProperty("log.outputDirectory");
  		
  		//PROPIEDADES DEL TUNEL SSH PARA CONEXION A LA BASE DE DATOS
  		ssh_host = prop.getProperty("ssh.host");
  		ssh_port = prop.getProperty("ssh.port");
  		ssh_user = prop.getProperty("ssh.user");
  		ssh_password = prop.getProperty("ssh.password");
  		  		
  		inputStream.close();		
	}
  		
	/**
	 * Method update the accounts from initial date
	 * 
	 * @param dateFrom
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws JSchException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
    private static void updateAccounts(String dateFrom) throws ClassNotFoundException, SQLException, JSchException, InstantiationException, IllegalAccessException {
    	
    	PreparedStatement stmt = null;
    	ResultSet rs = null;
		String query = "";
		int totalAccountsUpdated = 0;
		int totalAccountsConsulted = 0;
	            
        //ESTABLEZCO CONEXION A LA BASE DE DATOS
	    tMySql = new Tn3MySQL();
	    
	    if(withSsh.equalsIgnoreCase("Y")) {
	        System.out.println(dateFormat.format(new Date()) + " - Doing SSH tunnel to connect toward database...");
	        logger.info("Doing SSH tunnel to connect toward database...");
	      		
	        tMySql.doSshTunnel(ssh_user, ssh_password, ssh_host, Integer.parseInt(ssh_port), db_ipServer, Integer.parseInt(db_portServer), Integer.parseInt(db_portServer));
	    }
	    
        System.out.println(dateFormat.format(new Date()) + " - Connecting to database...");
        logger.info("Connecting to database...");	
        
        conn = tMySql.startConnection(db_ipServer, db_portServer, db_schema, db_user, db_password);
        
        //OBTENGO LAS CUENTAS CUYA FECHA DE CREACION SEA >= A LA FECHA DE PARAMETRO DE ENTRADA
        System.out.println(dateFormat.format(new Date()) + " - Getting the accounts...");
        logger.info("Getting the accounts...");
        
        query = "SELECT A.ID, A.DATECREATED, A.ALIAS, A.MSISDN, A.PROMOID, A.INDICATORMT, \r\n"
        		+ "		A.SUBSCRIPTIONCODE, A.SUBSCRIPTIONSTATUS, A.FLAGPREACTIVATION, A.OFFERINGTYPE,\r\n"
        		+ "        A.SOURCEACTIVATED, A.DATEACTIVATEDDOCOMO\r\n"
        		+ "FROM ACCOUNT A\r\n"
        		+ "WHERE DATE_FORMAT(A.DATECREATED,\"%Y-%m-%d\") >= '"+dateFrom+"'\r\n"
        		+ "order by DATECREATED DESC;";
		
        stmt = conn.prepareStatement(query);
		rs = stmt.executeQuery();
		  
		//System.out.println(query);
		while (rs.next()) {
			
			totalAccountsConsulted++;
			
			String idAccount = rs.getString("ID");
			String promoId = rs.getString("PROMOID");
			String indicatorMt = rs.getString("INDICATORMT");
			
			System.out.println ("###### Record No. " + totalAccountsConsulted + " ########");
			logger.info("###### Record No. " + totalAccountsConsulted + " ########");
			System.out.println ("ID: " + idAccount);
			logger.info("ID: " + idAccount);
			System.out.println ("DATECREATED: " + rs.getString("DATECREATED"));
			logger.info("DATECREATED: " + rs.getString("DATECREATED"));
			System.out.println ("ALIAS: " + rs.getString("ALIAS"));
			logger.info("ALIAS: " + rs.getString("ALIAS"));
			System.out.println ("MSISDN: " + rs.getString("MSISDN"));
			logger.info("MSISDN: " + rs.getString("MSISDN"));
			System.out.println ("PROMOID: " + promoId);
			logger.info("PROMOID: " + promoId);
			System.out.println ("INDICATORMT: " + indicatorMt);
			logger.info("INDICATORMT: " + indicatorMt);
			System.out.println ("DATEACTIVATEDDOCOMO: " + rs.getString("DATEACTIVATEDDOCOMO"));
			logger.info("DATEACTIVATEDDOCOMO: " + rs.getString("DATEACTIVATEDDOCOMO"));
						
			//OBTENGO EL ID DE LA TABLA PROMOTION
			String idPromotion = getIdPromotion(idAccount, promoId, indicatorMt);
			
			if (idPromotion != null) {
				if(!idPromotion.equals("")) {
					System.out.println ("ID PROMOTION: " + idPromotion);
					logger.info("ID PROMOTION: " + idPromotion);
				
					//ACTUALIZO EL PROMOID DE LA TABLA ACCOUNT
					updatePromoIdAccounts(idAccount, idPromotion);
				
					totalAccountsUpdated ++;
					System.out.println ("Record was updated.");
					logger.info("Record was updated.");
					
				}else {
					System.out.println ("ID PROMOTION wasn't founded");
					logger.info("ID PROMOTION wasn't founded");
					System.out.println ("Record wasn't updated.");
					logger.info("Record wasn't updated.");
				}
				
			}else {
				System.out.println ("ID PROMOTION wasn't founded");
				logger.info("ID PROMOTION wasn't founded");
				System.out.println ("Record wasn't updated.");
				logger.info("Record wasn't updated.");
			}
			
			System.out.println ("##############################");
			logger.info("##############################");
			
		}
						
		rs.close();
		stmt.close();
		  
		//MUESTRO TOTALES
		System.out.println(dateFormat.format(new Date()) + " - Total records consulted: " + totalAccountsConsulted);
		logger.info("Total records consulted: " + totalAccountsConsulted);	
		
		System.out.println(dateFormat.format(new Date()) + " - Total records updated: " + totalAccountsUpdated);
		logger.info("Total records updated: " + totalAccountsUpdated);	
		
		//CIERRO CONEXION CON LA BASE DE DATOS
		tMySql.finalizarConexion();
		System.out.println(dateFormat.format(new Date()) + " - Connection to database closed.");
		logger.info("Connection to database closed.");	
		
		if(withSsh.equalsIgnoreCase("Y")) {
			System.out.println(dateFormat.format(new Date()) + " - SSH tunnel toward database closed.");
	        logger.info("SSH tunnel toward database  closed.");	
			tMySql.closeSshTunnel();
		}
	    
	
    }	
   
    /**
     * Method get value of ID column from PROMOTION table
     * 
     * @param idAccount
     * @param promoID
     * @param indicatorMt
     * @return idPromotion
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws JSchException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private static String getIdPromotion(String idAccount, String promoID, String indicatorMt) throws ClassNotFoundException, SQLException, JSchException, InstantiationException, IllegalAccessException {
    	
    	PreparedStatement stmt = null;
    	ResultSet rs = null;
		String query = "";
		String idPromotion = "";
	            
        query = "SELECT MAX(ID) ID FROM PROMOTIONS P \r\n"
        		+ "WHERE P.PROMOID = '"+promoID+"'\r\n"
        		+ "AND P.INDICATORMT = '"+indicatorMt+"';";
        				
        stmt = conn.prepareStatement(query);
		rs = stmt.executeQuery();
		  
		//System.out.println(query);
		while (rs.next()) {
			
			//System.out.println (rs.getString("ID"));
						
			idPromotion = rs.getString("ID");
			
		}
						
		rs.close();
		stmt.close();
		
		return idPromotion;
		  
    }	
    
    /**
     * Method update the value of the PROMOID column in the ACCOUNT table
     * 
     * @param idAccount
     * @param idPromotion
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws JSchException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private static void updatePromoIdAccounts(String idAccount, String idPromotion) throws ClassNotFoundException, SQLException, JSchException, InstantiationException, IllegalAccessException {
    	
    	Statement stmt = conn.createStatement();
		String sql = "";
	            
		sql = "UPDATE ACCOUNT A SET PROMOID = "+idPromotion+" WHERE A.ID = "+idAccount+";";
		
		//System.out.println(sql);
		
		stmt.executeUpdate(sql);
						
		stmt.close();
    }	
    
}
