package com.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pojo.Participant;
import com.pojo.Security;

public interface ParticipantDAO {

	//Pooja
		public List<Security> getOpeningSecurities(int participantId);
		public double getOpeningFunds(int participantId);//existing balance of funds and shares
		public List<Security> getNetSecurities(int participantId);
		public double getNetFunds(int participantId);//netted value of funds and shares
		
		//Akanksha
		public boolean updateSecurityBalance(int participantId,List<Security> listOfSecurities);//
		public boolean updateFundBalance(int participantId,double funds);
		public boolean updateParticipant(int participantId, String emailId, String contactNumber);
		public HashMap<String,Integer> getParticipantIdMap();//should be called from servlet and added into participant object then can only other methods be called
		public boolean addParticipant(Participant participant);
		public boolean addLoginInfo(int participantId, String username, String password);
		public boolean updateLoginInfo(int participantId, String password);
		public Participant getParticipantById(int participantId);
		public Integer getLoginInfo(String username, String password); //login validation from db
		
		//Shafa
		public String getParticipantNameById(int participantId);	//For mapping clientId to its Name for UI
		public List<String> getAllParticipantName();
		public List<List<Integer>> perfomJoinBalanceSecurity();
		public Map<Integer,List<Integer>> getCorporateActionByParticipantId(int participant_id); //do it by joining of table
		public boolean updateParticipantBalance(int ParticipantId,int SecurityId,int SecurityQty);
//		public List<> getCorporateActionByParticipantId(Integer participant_id); //do it by joining of table
	
}

