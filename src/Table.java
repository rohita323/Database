import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Table {

	public String table_name;
	public int num_atts;
	public List<String[]> atts_details;
	public int num_records;
	public int old_num_of_records;
	
	Table(String name, HashMap mp )
	{
		table_name = name;
		
		atts_details = new ArrayList<String []>();
		
		String[] temp;
		Iterator it = mp.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
	        temp = new String[2];
	        temp[0] = (String)pairs.getKey(); //Attribute Name
	        temp[1] = (String)pairs.getValue(); //Attribute Type
	        atts_details.add(temp);
	        it.remove(); // avoids a ConcurrentModificationException
	    }
	    
	    num_atts = atts_details.size();
	}
	Table()
	{
		
	}

}
