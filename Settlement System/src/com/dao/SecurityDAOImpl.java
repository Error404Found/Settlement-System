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

import com.pojo.Security;

public class SecurityDAOImpl implements SecurityDAO {

	private Connection openConnection() {
		Connection connection=null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			System.out.println("Driver loaded succesfully");
			connection= DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe",
					"hr", "123");
			System.out.println("Connection obtained");
			
		} 
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return connection;
	}
	@Override
	public List<String> getAllSecurityName() {
		
		List<String> securitiesNames = new ArrayList<String>();
		String findAllSecurityName = "select security_name from securities";
		try {
			Statement st = openConnection().createStatement();
			ResultSet set = st.executeQuery(findAllSecurityName);
			while(set.next())
			{
				String securitiesName = set.getString("security_name");
				securitiesNames.add(securitiesName);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return securitiesNames;
	}

	@Override
	public HashMap<String, Integer> getSecurityIdMap() {
		// TODO Auto-generated method stub
		HashMap<String, Integer> securityIdName = new HashMap<String,Integer>();
		String securitiesInfo = "select * from securities";
		
		try {
			Statement st = openConnection().createStatement();
			ResultSet set = st.executeQuery(securitiesInfo);
			
			while(set.next())
			{
				String securityName = set.getString("security_name");
				Integer securityId = set.getInt("security_id");
				securityIdName.put(securityName, securityId);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return securityIdName;
	}


	@Override
	public HashMap<Integer,HashMap<Integer, Integer> > getAllCorporateAction() {
		// TODO Auto-generated method stub
		HashMap<Integer,HashMap<Integer, Integer>> hashMap=new HashMap<>();
		Connection connection=openConnection();
		String QUERY="select security_id,corporate_action,corporate_action_ratio from securities";
		try {
			PreparedStatement statement=connection.prepareStatement(QUERY);
			ResultSet set=statement.executeQuery();
			while(set.next()) {
				int security_id=set.getInt(1);
				int corporate_action=set.getInt(2);
				int corporate_action_ratio=set.getInt(3);
				HashMap<Integer, Integer> m=new HashMap();
				m.put(corporate_action, corporate_action_ratio);
				hashMap.put(security_id, m);
			}
			return hashMap;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return null;
	}

	@Override
	public boolean updateCorporateAction(int securityId,Security security) {
		// TODO Auto-generated method stub
		Connection connection=openConnection();
		String QUERY="update table securities set corporate_action=? and corporate_action_ratio=? where security_id=?";
		try {
			PreparedStatement statement=connection.prepareStatement(QUERY);
			statement.setInt(1, security.getCorporateAction());
			statement.setInt(2, security.getCorporateActionRatio());
			statement.setInt(3, securityId);
			int rowsUpdated=statement.executeUpdate();
			if(rowsUpdated==1)
				return true;
			else
				return false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return false;
	}



	@Override
	public boolean deleteCorporateAction(int securityId) {
		// TODO Auto-generated method stub
		Connection connection=openConnection();
		String QUERY="update securities set corporate_action=? and corporate_action_ratio=? where security_id=?";
		try {
			PreparedStatement statement=connection.prepareStatement(QUERY);
			statement.setInt(1, 0);
			statement.setInt(2, 0);
			statement.setInt(3, securityId);
			int rowsUpdated=statement.executeUpdate();
			if(rowsUpdated==1)
				return true;
			else
				return false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return false;
	}

	@Override
	public HashMap<Integer, Integer> getCorporateActionById(int security_id) {
		Connection connection=openConnection();
		HashMap<Integer, Integer> hashMap=new HashMap();
		String QUERY="select corporate_action,corporate_action_ratio from securities where security_id=?";
		try {
			PreparedStatement statement=connection.prepareStatement(QUERY);
			statement.setInt(1, security_id);
			ResultSet set=statement.executeQuery();
			while(set.next()) {
				int corporate_action=set.getInt(1);
				int corporate_action_ratio=set.getInt(2);
				hashMap.put(corporate_action, corporate_action_ratio);
			}
			return hashMap;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return null;
	}
	
	
	
	

}
