package com.dao;

import java.util.HashMap;
import java.util.List;

import com.pojo.Security;
import com.pojo.Transaction;

public interface SecurityDAO {
//Shafa
	
	public HashMap<String,Integer> getSecurityIdMap();
	
//Ashish	
	
	public boolean deleteCorporateAction(int securityId);//only do the corporsate_action as 0 and ratio as null dont delete the full security record
	HashMap<Integer, HashMap<Integer, Integer>> getAllCorporateAction();
	boolean updateCorporateAction(int securityId, Security security);
	HashMap<Integer, Integer> getCorporateActionById(int security_id);
	public List<String> getAllSecurityName();
	

}
