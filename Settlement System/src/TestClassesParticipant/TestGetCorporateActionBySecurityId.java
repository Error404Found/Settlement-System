package TestClassesParticipant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dao.ParticipantDAO;
import com.dao.ParticipantDaoImpl;

public class TestGetCorporateActionBySecurityId {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ParticipantDAO dao= new ParticipantDaoImpl();
		Map<Integer, List<Integer>> corporateActionSecId = new HashMap<>();
		corporateActionSecId=dao.getCorporateActionByParticipantId(102);
		System.out.println(corporateActionSecId);
	}

}
