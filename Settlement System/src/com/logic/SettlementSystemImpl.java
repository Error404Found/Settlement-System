package com.logic;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.dao.ParticipantDAO;
import com.dao.ParticipantDaoImpl;
import com.dao.TransactionDAO;
import com.dao.TransactionDAOImpl;
import com.pojo.Participant;
import com.pojo.Security;
import com.pojo.Transaction;

public class SettlementSystemImpl implements SettlementSystem {
	
	static double commissionRate=2.0d;
	
	public int findSecurity(List<Security> l1, Security s2)
	{
		for(int i=0;i<l1.size();i++)
		{
			if(l1.get(i).getSecurityId()==s2.getSecurityId())
				return i;
		}
		
		return -1;
	}

	public double findSimpleInterest(int shortage, double marketPrice, double penaltyRate)
	{
		double val = (double) 2/365; 
		//System.out.println(val);
		double actualRate = penaltyRate * val;
		//System.out.println(actualRate);
		double totAmount = shortage * marketPrice * actualRate;
		//System.out.println(totAmount);
		return totAmount;
	}
	
	@Override
	public HashMap<Integer,List> detectSecurityShortages(int participantId) {
	
		ParticipantDAO participantDAO = new ParticipantDaoImpl();
		List<Security> l1 =participantDAO.getOpeningSecurities(participantId); //opening SHARE balance
		List<Security> l2 =participantDAO.getNetSecurities(participantId); //net SHARE balance
		
		
		HashMap<Integer,List> hashMap = new HashMap();
		List l3;
		
		System.out.println("OPENING BALANCES");
		for(Security s: l1)
		{
			System.out.println(s.toString());
		}
		
		System.out.println("net BALANCES");
		for(Security s: l2)
		{
			System.out.println(s.toString());
		}
		
		//System.out.println("opening share balance list:"+l1);
		
		
		for(int i=0;i<l2.size();i++)
		{
			int val =findSecurity(l1, l2.get(i)); //find index
			System.out.println("INDEX OF SECURITY IS:"+val);
			if(val>=0)
			{
				//System.out.println("match found");
				
				if(l2.get(i).getSecurityQuantity()<0)
				{
					//System.out.println("need to give shares");
					
					if(l1.get(val).getSecurityQuantity()<(l2.get(i).getSecurityQuantity()*-1))
					{
						
						int shortage = (l2.get(i).getSecurityQuantity()*-1) - l1.get(val).getSecurityQuantity();
						System.out.println("SHARE Shortage detected:"+shortage);
						
						double fineForShortage = findSimpleInterest(shortage, 23.56, 7.0);
						
						l3 = new ArrayList();
						l3.add(shortage);
						l3.add(fineForShortage);
						
						hashMap.put(l1.get(val).getSecurityId(), l3);
						
					}
					else
					{
						System.out.println("No shortage FOR SHARES");
					}
				}
				else
				{
					System.out.println("NO chance for shortage");
				}
				
			}
			else
			{
				System.out.println("no match for index");
			}
		}
		
		System.out.println(hashMap);
		
		
		
		if(hashMap.isEmpty())
			return null;
		else
			return hashMap;
	}

	@Override
	public boolean performSettlement(int particiapntId) {
		
		ParticipantDAO participantDAO = new ParticipantDaoImpl();
		List<Security> l1 = participantDAO.getOpeningSecurities(particiapntId); //opening balance
		List<Security> l2 = participantDAO.getNetSecurities(particiapntId); //net balance
		

		//SHARE UPDATES
		for(int i=0;i<l2.size();i++)
		{
			int val =findSecurity(l1, l2.get(i));
			if(val>=0)
			{
				//System.out.println("match found");
				
				if(l2.get(i).getSecurityQuantity()<0)
				{
					//System.out.println("need to give shares");
					l1.get(val).setSecurityQuantity(l1.get(val).getSecurityQuantity()-((l2.get(i).getSecurityQuantity()*-1)));
					//System.out.println("new value"+l1.get(val).getSecurityQuantity());
				}
				else
				{
					//System.out.println("need to add shares");
					l1.get(val).setSecurityQuantity(l1.get(val).getSecurityQuantity()+l2.get(i).getSecurityQuantity());
					//System.out.println("new value"+l1.get(val).getSecurityQuantity());
				}
				
			}
			else
			{
				System.out.println("No such entry");
			}
		}
		
	//	System.out.println("Updated share values");
		
	/*	for(Security s : l1)
		{
			System.out.println();
			System.out.print(s.getSecurityId());
			System.out.print(s.getSecurityName());
			System.out.print(s.getSecurityQuantity());
			System.out.println();
		}*/
		
		//FUND UPDATES
		double balanceFund =participantDAO.getOpeningFunds(particiapntId);
		double netFund = participantDAO.getNetFunds(particiapntId);
		
		double finalFunds = balanceFund - netFund;
		for(Security security :l1)
		{
			participantDAO.updateParticipantBalance(particiapntId, security.getSecurityId(),security.getSecurityQuantity());
		}
		
		participantDAO.updateFundBalance(particiapntId, finalFunds);
		return true;
		
	}

	@Override
	public double detectFundShortages(int participantId) {
		
		ParticipantDAO participantDAO = new ParticipantDaoImpl();
		double balanceFund =participantDAO.getOpeningFunds(participantId);
		double netFund = participantDAO.getNetFunds(participantId);
		double fundShortage = 0;
		if(netFund<0)
		{
			System.out.println("participant HAS negative obligation for funds");
			
			if((balanceFund-(netFund*(-1)))<0)
			{
				fundShortage =(netFund*(-1)-balanceFund);
				System.out.println("Shortage detedted:"+fundShortage);
			}
		}
		else
		{
			System.out.println("positive obligation for funds");
		}
		
		
		return fundShortage;
	}

	@Override
	public void generateObligationReport(int participantId) {
	
		ParticipantDAO participantDAO = new ParticipantDaoImpl();
		Participant par =participantDAO.getParticipantById(participantId);
		//par.getParticipantName(participantId);
		
		List<Security> list =participantDAO.getNetSecurities(participantId);
		double netFunds = participantDAO.getNetFunds(participantId);
		System.out.println("\n\n");
		System.out.println("SETTLEMENT HOUSE OBLIGATION REPORT");
		System.out.println("******************************************");
		System.out.println("CLEARING MEMBER ID:"+par.getParticipantId());
		System.out.println("CLEARING MEMBER NAME:"+par.getParticipantName());
		System.out.println("Contact Number:"+par.getContactNumber());
		System.out.println("******************************************");
		System.out.println("SHARE OBLIGATION");
		
		for(Security s : list)
		{
			System.out.println();
			System.out.print("Security Id:"+s.getSecurityId()+" ");
			System.out.print("Security Name:"+s.getSecurityName()+" ");
			System.out.print("Security Quantity:"+s.getSecurityQuantity()+" ");
		}
		System.out.println("\n");
		System.out.println("******************************************");
		System.out.println("FUND OBLIGATION");
		
		System.out.println("Total funds obligation"+netFunds);
		
	}

	@Override
	public void performNettingProcedure() {
		// TODO Auto-generated method stub
				try {
				HashMap<Integer, HashMap<Integer, Integer>> securitiesNettingMap =new HashMap<>();
				HashMap<Integer, Double> fundsNettingMap=new HashMap<>();
				TransactionDAO transactionDAO=new TransactionDAOImpl();
				List<Transaction> transactions=transactionDAO.getAllTransaction();
				//System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"+transactions.size());
				HashMap<Integer, Integer> map;
				//System.out.println("Transactions::");
				//System.out.println(transactions);
				for(Transaction transaction:transactions) {
					
					map=null;
					if(securitiesNettingMap.containsKey(transaction.getBuyerId())) {
						map=(securitiesNettingMap.get(transaction.getBuyerId()));
						fundsNettingMap.replace(transaction.getBuyerId(),fundsNettingMap.get(transaction.getBuyerId())-transaction.getTransactionAmount());
						if(map.containsKey(transaction.getSecurityId())) {
							map.replace(transaction.getSecurityId(), map.get(transaction.getSecurityId())+transaction.getSecurityQuantity());
						}else {
							map.put(transaction.getSecurityId(), transaction.getSecurityQuantity());
						}
						map=null;
					}else {
						map=new HashMap<>();
						fundsNettingMap.put(transaction.getBuyerId(), (-1)*transaction.getTransactionAmount());
						map.put(transaction.getSecurityId(), transaction.getSecurityQuantity());
						securitiesNettingMap.put(transaction.getBuyerId(), map);
						map=null;
					}
					
					if(securitiesNettingMap.containsKey(transaction.getSellerId())) {
						map=(securitiesNettingMap.get(transaction.getSellerId()));
						fundsNettingMap.replace(transaction.getSellerId(),fundsNettingMap.get(transaction.getSellerId())+transaction.getTransactionAmount());
						if(map.containsKey(transaction.getSecurityId())) {
							if(transaction.getSecurityId()==1 && transaction.getSellerId()==3)
							{
								System.out.println("************************Inside for Apple for Citi*********************");
							}
							
							map.replace(transaction.getSecurityId(), map.get(transaction.getSecurityId())-transaction.getSecurityQuantity());
						}else {
							map.put(transaction.getSecurityId(), (-1)*transaction.getSecurityQuantity());
						}
						map=null;
					}else {
						map=new HashMap<>();
						fundsNettingMap.put(transaction.getSellerId(), transaction.getTransactionAmount());
						map.put(transaction.getSecurityId(), (-1)*transaction.getSecurityQuantity());
						securitiesNettingMap.put(transaction.getSellerId(), map);
						map=null;
					}	
					//System.out.println(securitiesNettingMap);
				}	
				transactionDAO.generateNettingTable(securitiesNettingMap);
				transactionDAO.generateNettingFundsTable(fundsNettingMap);
				
				///	transactionDAO.updateFunds(fundsNettingMap);
				System.out.println("Final   map:"+securitiesNettingMap);
				System.out.println("Final   map:"+fundsNettingMap);
			}	
				catch (NullPointerException e) {
					// TODO: handle exception
					e.printStackTrace();
				}
				
		
	}

	@Override
	public void handleCorporateAction() {
		
		ParticipantDAO dao = new ParticipantDaoImpl();
		List<List<Integer>> balanceSecurityJoin=dao.perfomJoinBalanceSecurity();	//make performJoin.. func in Dao
		
		for(List<Integer> list:balanceSecurityJoin) 
		{
			int corporateAction=list.get(2);
			
			if(corporateAction==1)//Stock dividends
			{
				int updateqty=Math.round((float)list.get(4)/list.get(3));
				list.set(4, list.get(4)+updateqty);
				
			}
			else if(corporateAction==2)//Stock split
			{
				int updateqty=list.get(4)*list.get(3);
				list.set(4, updateqty);
			}
			else if(corporateAction==3)//Rights
			{
				if(list.get(5)==1)// 1 means perform the rights
				{
					//calculating random marketPrice-provided by Rights Provider
					double randomDouble = Math.random();
					int maxVal=list.get(6)+10;
					int minVal=list.get(6)-10;
					randomDouble = randomDouble*maxVal+minVal;
					
					//updating qty
					int updateqty=Math.round((float)list.get(4)/list.get(3));
					list.set(4, list.get(4)+updateqty);
					
					//double updateFund = updateqty*randomDouble;
					double updateFund = updateqty*list.get(6);
					
					if(dao.updateFundBalance(list.get(0),dao.getOpeningFunds(list.get(0))-updateFund))
					{
						System.out.println("Rights performed successfully");
					}
					else
					{
						System.out.println("Rights failure");
					}
					
				}
			}
			
			//updating here
			if(dao.updateParticipantBalance(list.get(0), list.get(1), list.get(4))) {
				System.out.println("corporate balance updated for participant:"+list.get(0));
			}
			else {
				System.out.println("corporate balance balance updation for participant:"+list.get(0)+" failed");
			}
			
		}
	}


	@Override
	public void generateCostofSelletmentReceipt(int participantId) {
		// TODO Auto-generated method stub
		
		ParticipantDAO dao = new ParticipantDaoImpl();
		//participant-   1.shortage penalty ,kya kya shortage,corporate action for each share,cost of settltement
		
		Map<Integer, List> map =detectSecurityShortages(participantId);
		double totalShortagePenalty=0.0f;
		if(map.size()!=0) 
		{
			System.out.println("--------------Shortages---------------");
			for (Map.Entry<Integer,List> entry : map.entrySet())
			{
				System.out.println("SecurityId="+entry.getKey());
				System.out.println("ShortageAmt="+entry.getValue().get(0));
				System.out.println("ShortagePenalty="+entry.getValue().get(1));
				totalShortagePenalty=totalShortagePenalty+(double)entry.getValue().get(1);
			}
		}
		System.out.println("Total Security Shortage Penalty="+totalShortagePenalty);
		System.out.println("Total Fund Shortage penalty="+detectFundShortages(participantId));
		
		
		Map<Integer, List<Integer>> corporateActionSecId = dao.getCorporateActionByParticipantId(participantId);
		if(corporateActionSecId.size()!=0)
		{
			System.out.println("--------Corporate Actions Executed----------");
			for (Map.Entry<Integer,List<Integer>> entry : corporateActionSecId.entrySet())
			{
				//s.security_id,s.corporate_action,s.corporate_action_ratio\r\n
				System.out.println("SecurityId="+entry.getKey());
				System.out.println("CorporateAction="+entry.getValue().get(0));
				System.out.println("CorporateActionRatio="+entry.getValue().get(1));
			}
		}
		
		//Cost of settlement 
		TransactionDAO dao2 = new TransactionDAOImpl();
		int transactionListSize = dao2.getTransactionByParticipantId(participantId).size();
		double costOfSettlement = transactionListSize*commissionRate;
		System.out.println("Cost of settlement = "+costOfSettlement);
	}


}


