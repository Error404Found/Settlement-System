package com.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pojo.Transaction;

public class TransactionDAOImpl implements TransactionDAO {

	private Connection openConnection()
	{
		Connection connection = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver"); //driver has been registered
			//System.out.println("Driver loaded succesfully");
			//"jdbc:data_base:install_server:port/database"
			
			connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe","system","mitali");
			//System.out.println("Connection obtained");
			    
		}
		catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}
		catch (SQLException e) 
		{
				e.printStackTrace();
		}
		
		return connection;
	}
	
	@Override
	public List<Transaction> getAllTransaction() {
		
		Connection connection = openConnection();
		List<Transaction> listOfTransactions = new ArrayList<Transaction>();
		
		try {
		Statement statement = connection.createStatement();
		
		ResultSet rs = statement.executeQuery("select* from TRANSACTIONS");
		
			while(rs.next())
			{
				int tranId = rs.getInt("transaction_id");
				int ssinId = rs.getInt("SSIN_id");
				Integer sellerId = rs.getInt("seller_id");
				Integer buyerId = rs.getInt("buyer_id");
				int securityId = rs.getInt("security_id");
				int securityQty = rs.getInt("security_quantity");
				double securityPrice = rs.getFloat("share_price");
				double transactionAmt = securityQty*securityPrice;
				
				Transaction transaction = new Transaction(tranId, ssinId, sellerId, buyerId, securityId, securityQty, securityPrice, transactionAmt);
				listOfTransactions.add(transaction);
			}
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println(listOfTransactions.size());
		return listOfTransactions;
	}

	@Override
	public List<Transaction> getTransactionByParticipantId(int participantId) {
		
		Connection connection = openConnection();
		List<Transaction> listOfTransactionsByID = new ArrayList<Transaction>();
		
		try {
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery("select* from TRANSACTIONS where seller_id="+participantId+"or buyer_id="+participantId);
	
		while(rs.next())
		{
			int tranId = rs.getInt("transaction_id");
			int ssinId = rs.getInt("SSIN_id");
			int sellerId = rs.getInt("seller_id");
			int buyerId = rs.getInt("buyer_id");
			int securityId = rs.getInt("security_id");
			int securityQty = rs.getInt("transaction_id");
			double securityPrice = rs.getFloat("share_price");
			double transactionAmt = securityQty*securityPrice;
			
			Transaction transaction = new Transaction(tranId, ssinId, sellerId, buyerId, securityId, securityQty, securityPrice, transactionAmt);
			listOfTransactionsByID.add(transaction);
		}
	} 
	catch (SQLException e) {
		e.printStackTrace();
	}		
		return listOfTransactionsByID;
 }
		
	@Override
	public boolean generateNettingTable(HashMap<Integer, HashMap<Integer, Integer>> hashMap) {
		//for securities
		//System.out.println("Netting share map:"+hashMap);
		
		boolean added = false;
		int participantId=-1;
		int securityId=-1;
		int securityNetVal=0;
		String INSERT_NETTING;
		
		Connection connection = openConnection();
	    PreparedStatement ps;   
	    
		for(Map.Entry participant: hashMap.entrySet())
		{
			participantId =(int) participant.getKey();
			HashMap<Integer,Integer> hm =(HashMap) participant.getValue();
			
			for(Map.Entry balances: hm.entrySet())
			{
				securityId =(int) balances.getKey();
				securityNetVal =(int) balances.getValue();
			
			
			INSERT_NETTING = "INSERT INTO securities_netting_result VALUES (?,?,?)";
			try {
				ps = connection.prepareStatement(INSERT_NETTING);
				ps.setInt(1, participantId);
				ps.setInt(2, securityId);
				ps.setInt(3, securityNetVal);
				
				int rows_inserted =ps.executeUpdate();		    
			    //System.out.println("Rows added:"+rows_inserted);
			    
			    if(rows_inserted>0)
			    {
			    	added = true;
			    }
				
			} 
			
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
			
		}
		
		return added;
	}
	
	
	@Override
	public boolean generateNettingFundsTable(HashMap<Integer, Double> fundsNettingMap) {
		// TODO Auto-generated method stub
		Connection connection = openConnection();
		String QUERY="insert into funds_netting_result values(? , ?)";
		try {
			for (Map.Entry<Integer,Double> entry : fundsNettingMap.entrySet()) {
				//System.out.println("EntryKey:"+entry.getKey()+"   "+"EntryValue:"+entry.getValue());
				PreparedStatement statement=connection.prepareStatement(QUERY);
				statement.setInt(1, entry.getKey());
				statement.setDouble(2, entry.getValue());
				statement.executeUpdate();
			}
			
			return true;
		} catch (SQLException e) {
			// TODO: handle exception
			return false;
		}
		
	}

	@Override
	public boolean addTransaction(Transaction transaction) {
		
		boolean added = false;
		
		Connection connection = openConnection();
		String INSERT_TRANSACTION ="INSERT INTO transactions VALUES(?,?,?,?,?,?,?)";
	    PreparedStatement ps;    
		
try {
		ps = connection.prepareStatement(INSERT_TRANSACTION);
		
	    ps.setInt(1,transaction.getTransactionId());
	    ps.setInt(2,transaction.getSSIN_Id());
	    ps.setInt(3,transaction.getSellerId());
	    ps.setInt(4,transaction.getBuyerId());
	    ps.setInt(5,transaction.getSecurityId());
	    ps.setInt(6,transaction.getSecurityQuantity());
	    ps.setDouble(7,transaction.getSecurityRate());
	    
	    
	    int rows_inserted =ps.executeUpdate();		    
	   // System.out.println("Rows added:"+rows_inserted);
	    
	    if(rows_inserted>0)
	    {
	    	added = true;
	    }
	    
}		
catch (SQLException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
	 
		return added;
}

	
	@Override
	public boolean updateTransaction(Transaction transaction) {
		
		boolean updated = false;
		Connection connection = openConnection();
		String UPDATE_TRANSACTION ="UPDATE transactions SET security_quantity = ? WHERE transaction_id = ?";
	    PreparedStatement ps;    
	    
	    try {
			ps = connection.prepareStatement(UPDATE_TRANSACTION);
			
			ps.setInt(1,transaction.getSecurityQuantity());
		    ps.setInt(2,transaction.getTransactionId());
		  
		    int rows_updated =ps.executeUpdate();		    
		   // System.out.println("Rows updated:"+rows_updated);
		    
		    if(rows_updated>0)
		    {
		    	updated = true;
		    }
		    
	    }		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return updated;
	}

	@Override
	public boolean deleteTransaction(int transactionId) {
		
		boolean deleted = false;
		
		Connection connection = openConnection();
		String DELETE_TRANSACTION ="DELETE FROM TRANSACTIONS WHERE transaction_id=?";
	    PreparedStatement ps;    
	    
	    try {
			ps = connection.prepareStatement(DELETE_TRANSACTION);
			ps.setInt(1,transactionId);
		  
		    int rows_deleted =ps.executeUpdate();		    
		    //System.out.println("Rows deleted:"+rows_deleted);
		    
		    if(rows_deleted>0)
		    {
		    	deleted = true;
		    }
		    
	    }		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return deleted;
	}

}
