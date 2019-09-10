package com.dao;

import java.io.ObjectInputStream.GetField;
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

import com.pojo.Participant;
import com.pojo.Security;

public class ParticipantDaoImpl implements ParticipantDAO
{
	private Connection openConnection() {
		Connection connection=null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			System.out.println("Driver loaded succesfully");
			connection= DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe",
					"system", "mitali");
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
	
	

public List<Security> getOpeningSecurities(int participantId) { {
		
		List<Security> sec_list=new ArrayList<Security>();
		String GET_OPENING_SECURITIES="select participant_balance.participant_id,participant_balance.security_id,participant_balance.security_quantity,securities.security_name from participant_balance join securities on participant_balance.security_id=securities.security_id where participant_id=?";  
		
		try {
			
			PreparedStatement ps = openConnection().prepareStatement(GET_OPENING_SECURITIES);
			ps.setInt(1, participantId);
			
			
			ResultSet set= ps.executeQuery();
			while(set.next()) {
				int securityId=set.getInt("security_id");
				String securityName=set.getString("security_name");
				int securityQuantity=set.getInt("security_quantity");
//				int corporateAction=set.getInt("corporate_action");
//				int corporateActionRatio=set.getInt("corporate_action_ratio");
//				int performRights =set.getInt("perform_rights");
//				
				
				Security sec=new Security(securityName,securityId,securityQuantity);
				sec_list.add(sec);
			}
			System.out.println("List size= "+sec_list.size());
			
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return sec_list;
		
	
	}
	}

public List<Security> getNetSecurities(int participantId) {
	
	List<Security> sec_list=new ArrayList<Security>();
	String GET_NET_SECURITIES="select securities_netting_result.participant_id, securities_netting_result.security_id,securities_netting_result.securities_amount,securities.security_name from securities_netting_result join securities on securities_netting_result.security_id=securities.security_id where participant_id=?"; 
			 
		
	try {
		
		PreparedStatement ps = openConnection().prepareStatement(GET_NET_SECURITIES);
		ps.setInt(1, participantId);
		ResultSet set= ps.executeQuery();
		while(set.next()) {
			int sid=set.getInt("security_id");
			String sname=set.getString("security_name");
			int qty=set.getInt("securities_amount");
			
			
			Security sec=new Security(sname,sid,qty);
			sec_list.add(sec);
		}
		System.out.println("List size= "+sec_list.size());
		
	
	} catch (SQLException e) {
		e.printStackTrace();
	}
	return sec_list;
	

	
}




@Override
public double getOpeningFunds(int participantId) {
	double funds=0;
	String GET_OPENING_FUNDS="select funds from participants where participant_id=?";
try {
		
		PreparedStatement ps = openConnection().prepareStatement(GET_OPENING_FUNDS);
		ps.setInt(1, participantId);
		ResultSet set= ps.executeQuery();
		while(set.next()) {
			 funds=set.getFloat("funds");
		}
} catch (SQLException e) {
	e.printStackTrace();
}
	return funds;
	
}

@Override
public double getNetFunds(int participantId) {
	double funds=0;
	String GET_NET_FUNDS="select funds from funds_netting_result where participant_id=?";
try {
		
		PreparedStatement ps = openConnection().prepareStatement(GET_NET_FUNDS);
		ps.setInt(1, participantId);
		ResultSet set= ps.executeQuery();
		while(set.next()) {
			 funds=set.getFloat("funds");
		}
} catch (SQLException e) {
	e.printStackTrace();
}
	return funds;
	
	
	
}




	@Override
	public boolean updateSecurityBalance(int participantId, List<Security> listOfSecurities) {
		// TODO Auto-generated method stub
		
		boolean sharesUpdated=false;
		
		
		try {
			//to delete existing records for that participant
			String DELETE_SHARE_BALANCE="delete from participant_balance where participant_id=?";
			PreparedStatement ps = openConnection().prepareStatement(DELETE_SHARE_BALANCE);
			int rowDeleted=0;
			ps.setInt(1, participantId);
			System.out.println(rowDeleted+ "  ");
			rowDeleted=ps.executeUpdate();
			System.out.println("existing shares deleted");
			
			
			//to add updated list
			String INSERT_INTO_PARTICIPANT_BALANCE="Insert into participant_balance values(?,?,?)";
			PreparedStatement ps1=openConnection().prepareStatement(INSERT_INTO_PARTICIPANT_BALANCE);
			int size=listOfSecurities.size();
			System.out.println("size =" +size);
			int rowInserted=0;
			int i=0;
			while(i<size)
			{
				ps1.setInt(1, participantId);
				ps1.setInt(2, listOfSecurities.get(i).getSecurityId());
				ps1.setInt(3, listOfSecurities.get(i).getSecurityQuantity());
				
				rowInserted=ps1.executeUpdate();
				i++;
				System.out.println("security added "+ i);
				rowInserted++;
			}
			if (rowInserted!=0)
			{
				sharesUpdated=true;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return sharesUpdated;
	}



	@Override
	public boolean updateFundBalance(int participantId, double funds) {
		// TODO Auto-generated method stub
		int rowInserted=0;
		boolean fundsUpdated=false;
		
		try {
			String UPDATE_FUNDS="update Participants set funds=? where participant_Id=?";
			PreparedStatement ps=openConnection().prepareStatement(UPDATE_FUNDS);
			ps.setDouble(1, funds);
			ps.setInt(2, participantId);
			rowInserted=ps.executeUpdate();
			
			if (rowInserted!=0)
			{
				fundsUpdated=true;
			}
			
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return fundsUpdated;
	}

	@Override
	public boolean updateParticipant(int participantId, String emailId, String contactNumber) {
		// TODO Auto-generated method stub
		int rowInserted=0;
		boolean participantUpdated=false;
		
		try {
			String UPDATE_PARTICIPANT="update Participants set email_id=? , contact_no=? where participant_Id=?";
			PreparedStatement ps=openConnection().prepareStatement(UPDATE_PARTICIPANT);
			System.out.println("ps run");
			
			ps.setString(1, emailId);
			ps.setString(2, contactNumber);
			ps.setInt(3, participantId);
			rowInserted=ps.executeUpdate();
			
			if (rowInserted!=0)
			{
				participantUpdated=true;
			}
			
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return participantUpdated;
		
	}

	@Override
	public boolean addParticipant(Participant participant) {
		// TODO Auto-generated method stub
		int rowInserted=0;
		boolean participantAdded=false;
		
		try {
			String INSERT_INTO_PARTICIPANT="Insert into Participants values(?,?,?,?,?,?)";
			PreparedStatement ps=openConnection().prepareStatement(INSERT_INTO_PARTICIPANT);
			
			ps.setInt(1, participant.getParticipantId());
			ps.setString(2, participant.getParticipantName());
			ps.setString(3, participant.getEmailId());
			ps.setString(4, participant.getContactNumber());
			ps.setDouble(5, participant.getFunds());
			ps.setDouble(6, participant.getFeeForSettlement());
			rowInserted=ps.executeUpdate();
			System.out.println("row inserted in participant table");
			
			
			String INSERT_INTO_PARTICIPANT_BALANCE="Insert into participant_balance values(?,?,?)";
			PreparedStatement ps1=openConnection().prepareStatement(INSERT_INTO_PARTICIPANT_BALANCE);
			int size=participant.getListOfSecurities().size();
			System.out.println("size =" +size);
			rowInserted=0;
			int i=0;
			while(i<size)
			{
				ps1.setInt(1, participant.getParticipantId());
				ps1.setInt(2, participant.getListOfSecurities().get(i).getSecurityId());
				ps1.setInt(3, participant.getListOfSecurities().get(i).getSecurityQuantity());
				
				rowInserted=ps1.executeUpdate();
				i++;
				System.out.println("security added "+ i);
				rowInserted++;
			}
			if (rowInserted!=0)
			{
				participantAdded=true;
			}
			
		} 
		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return participantAdded;
	}

	@Override
	public boolean addLoginInfo (int participantId, String username, String password) {
		// TODO Auto-generated method stub
		int rowInserted=0;
		boolean loginInfoAdded=false;
		
		try {
			String INSERT_INTO_LOGIN_INFO="Insert into Login_info values(?,?,?)";
			PreparedStatement ps=openConnection().prepareStatement(INSERT_INTO_LOGIN_INFO);
			
			ps.setInt(1, participantId);
			ps.setString(2, username);
			ps.setString(3, password);
			rowInserted=ps.executeUpdate();
			System.out.println("row inserted in login info table");
			
			if (rowInserted!=0)
			{
				loginInfoAdded=true;
			}
			
		} 
		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return loginInfoAdded;
	}

	@Override
	public boolean updateLoginInfo(int participantId, String password) {
		// TODO Auto-generated method stub
		boolean isUpdated=false;
		String LOGIN_INFO_UPDATE="update login_info set password=? where participant_id=?";
		try {
			PreparedStatement ps=openConnection().prepareStatement(LOGIN_INFO_UPDATE);
			ps.setString(1, password);
			ps.setInt(2,participantId);
			int rows=ps.executeUpdate();
			if(rows>0) {
				isUpdated=true;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return isUpdated;
	}

	@Override
	public Participant getParticipantById(int participantId) {
		// TODO Auto-generated method stub
		Participant participant=null;
		
		String FIND_PARTICIPANT="select * from Participants where participant_id=?";
		try {
			String participantName="";
			String emailId="";
			String contactNumber="";
			double funds=0;
			double feeForSettlement=0;
			List<Security> listOfSecurities=new ArrayList<>();
			
			PreparedStatement ps = openConnection().prepareStatement(FIND_PARTICIPANT);
			ps.setInt(1, participantId);
			ResultSet set= ps.executeQuery();
			while(set.next()) {
				 
				participantName= set.getString("participant_name");
				emailId= set.getString("email_ID");
				contactNumber=set.getString("contact_no");
				funds=set.getDouble("funds");
				feeForSettlement=set.getDouble("fee_for_settlement");
				System.out.println("details obtained from participant");
			}
			
			String FIND_PARTICIPANT_BALANCE="select * from Participant_balance where participant_id=?";
			PreparedStatement ps1 = openConnection().prepareStatement(FIND_PARTICIPANT_BALANCE);
			ps1.setInt(1, participantId);
			ResultSet set1= ps1.executeQuery();
			int i=0;
			while(set.next()) 
			{
				listOfSecurities.get(i).setSecurityId(set1.getInt("security_id"));
				listOfSecurities.get(i).setSecurityQuantity(set1.getInt("security_quantity"));
				i++;
				System.out.println("i="+i);
			}
			
			participant=new Participant(participantId, participantName, emailId, contactNumber, funds, feeForSettlement, listOfSecurities);
			
			System.out.println("participant=\n"+ participant.toString());
		} 
		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return participant;
	}

	@Override
	public Integer getLoginInfo(String username, String password) {
		// TODO Auto-generated method stub
		int participantId;
		Integer intObj=null;
		String GET_LOGIN_INFO="select participant_id from login_info where username=? and password=?";
		try {
			PreparedStatement ps = openConnection().prepareStatement(GET_LOGIN_INFO);
			ps.setString(1, username);
			ps.setString(2, password);
			participantId=ps.executeUpdate();
			intObj=new Integer(participantId);
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		Integer intObj=new Integer(participantId);
		
		return intObj;
	}

	@Override
	public String getParticipantNameById(int participantId) {
		// TODO Auto-generated method stub
		String participantName=null;
		String findParticipant = "select participant_name from participants where participant_id=?";
		
		try {
			PreparedStatement ps = openConnection().prepareStatement(findParticipant);
			ps.setInt(1, participantId);
			ResultSet set = ps.executeQuery();
			
			while(set.next())
			{
				participantName=set.getString("participant_name");
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return participantName;
	}

	@Override
	public List<String> getAllParticipantName() {
		// TODO Auto-generated method stub
		List<String> participantNames = new ArrayList<String>();
		String findAllParticipantName = "select participant_name from participants";
		try {
			Statement st = openConnection().createStatement();
			ResultSet set = st.executeQuery(findAllParticipantName);
			while(set.next())
			{
				String ParticipantName = set.getString("participant_name");
				participantNames.add(ParticipantName);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return participantNames;
	}



	@Override
	public HashMap<String, Integer> getParticipantIdMap() {
		// TODO Auto-generated method stub
		HashMap<String, Integer> participantIdName = new HashMap<String,Integer>();
		String PARTICIPANT_INFO = "select * from participants";
		
		try {
			Statement st = openConnection().createStatement();
			ResultSet set = st.executeQuery(PARTICIPANT_INFO);
			
			while(set.next())
			{
				String participantName = set.getString("participant_name");
				Integer participantId = set.getInt("participant_id");
				participantIdName.put(participantName, participantId);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return participantIdName;
	}


	@Override
	public List<List<Integer>> perfomJoinBalanceSecurity() {
		
		List<List<Integer>> joinedList = new ArrayList<List<Integer>>();
		
		String joinBalanceSecurity = "select p.participant_id,s.security_id,s.corporate_action,s.corporate_action_ratio,p.security_quantity,\r\n" + 
				"s.perform_rights,s.default_market_price\r\n" + 
				"from participant_balance p\r\n" + 
				"JOIN securities s on p.security_id=s.security_id";
		
		Statement st;
		try {
			st = openConnection().createStatement();
			ResultSet set = st.executeQuery(joinBalanceSecurity);
			
			while(set.next())
			{
				List<Integer> list=new ArrayList<Integer>();
				list.add(set.getInt(1));
				list.add(set.getInt(2));
				list.add(set.getInt(3));
				list.add(set.getInt(4));
				list.add(set.getInt(5));
				list.add(set.getInt(6));
				list.add(set.getInt(7));
				
				joinedList.add(list);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return joinedList;
	}


	
	@Override
	public Map<Integer, List<Integer>> getCorporateActionByParticipantId(int participant_id) {
		// TODO Auto-generated method stub
		Map<Integer, List<Integer>> corporateActionSecId = new HashMap<>();
		
		String joinBalanceSecurity = "select p.participant_id,s.security_id,s.corporate_action,s.corporate_action_ratio\r\n" + 
				"from participant_balance p\r\n" + "JOIN securities s on p.security_id=s.security_id";
		
		Statement st;
		try {
			st = openConnection().createStatement();
			ResultSet set = st.executeQuery(joinBalanceSecurity);
			while(set.next())
			{
				List<Integer> list = new ArrayList<>();
				list.add(set.getInt(3));
				list.add(set.getInt(4));
				corporateActionSecId.put(set.getInt(2), list);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return corporateActionSecId;
	}



	@Override
	public boolean updateParticipantBalance(int participantId, int securityId, int securityQty) {
		// TODO Auto-generated method stub
		int rowInserted=0;
		boolean participantUpdated=false;
		
		try {
			String UPDATE_PARTICIPANT="update Participant_balance set security_quantity=? where security_id=? and participant_Id=?";
			PreparedStatement ps=openConnection().prepareStatement(UPDATE_PARTICIPANT);
			System.out.println("ps run");
			
			ps.setInt(2, securityId);
			ps.setInt(1, securityQty);
			ps.setInt(3, participantId);
			rowInserted=ps.executeUpdate();
			
			if (rowInserted!=0)
			{
				participantUpdated=true;
			}
			
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return participantUpdated;
	}

	

	
}
