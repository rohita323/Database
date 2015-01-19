import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



public class DBSystem {
	
	
	private int PAGESIZE;
	private int NUM_PAGES;
	private String PATH_FOR_DATA;
	private List<String> table_names;
	private List<Table> populated_tables;
	private List<Page> disk;
	private int page_num;
	public static HashMap<String, Table> catalogue ;
	
	
	DBSystem()
	{
		page_num = 0;
		populated_tables = new ArrayList<Table>();
		disk = new ArrayList<Page>();
		catalogue = new HashMap<String,Table>();
	}
	
	
	
	public void readConfig(String configFilePath) {
		
		HashMap config ;
		table_names = new ArrayList<String>();
		
		/* Hashmap which has the configurations - 
		 	1.Path_for_data
		 	2.Page Size
		 	3.Num_Pages
		 	4.Hashmaps containing Table meta data with key as Table Name and 
		 	  a Hashmap as value		 	  
		*/ 
		 config = new HashMap();
		 
    	 BufferedReader br = null;
    	 
	     try {
	    	br = new BufferedReader(new FileReader(configFilePath));
	        String line = br.readLine();
	        String[] splited = null;
	        int l;

	        while (line != null) {
	            splited = line.split("\\s+");
	            if(splited[0].equals("BEGIN"))
	            {
	            	//to Extract table Name which is just after BEGIN
	            	String table_name = br.readLine();
	                table_name = table_name.replace("\n", "").replace("\r", "");
            		splited = table_name.split("\\s+");
            		
            		if(splited.length != 1)
            		{
            			System.out.println("Wrong Config File Structure.");
            			System.exit(1);
            		}
            		
            		table_names.add(table_name);
            		config.put(table_name, new HashMap());
            		
            		line = br.readLine();
	            	while(!line.equals("END") && line != null)
	            	{	            		
	            		splited = line.split("\\s*,\\s*");
	            		HashMap temp = (HashMap) config.get(table_name);
	            		temp.put(splited[0], splited[1]);	            		
	            		line = br.readLine();
	            		
	            	}
	            }
	            else
	            {
	            	l = splited.length;
	            	if(l==2)
	            	{
	            		// Inserting Key-Value pairs in HashMap namely - PAGE_SIZE, NUM_Pages and Path for data
	            		config.put(splited[0].toUpperCase(), splited[1].toLowerCase());
	            		
	            	}
	            }
	            line = br.readLine();
	            
	        }
	        
	        br.close();
		    } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	     
	     PAGESIZE = Integer.parseInt((String) config.get("PAGESIZE"));
	     NUM_PAGES =Integer.parseInt((String) config.get("NUM_PAGES"));
	     PATH_FOR_DATA =(String) config.get("PATH_FOR_DATA");
	     create_system_catalogue(config);
		
		
	}

	

	public void populatePageInfo() {
		
		System.out.println("Populating PageInfo");
		Iterator it = table_names.iterator();
		Table t = null;
		while(it.hasNext())
		{
			read_file_into_pages((String)it.next());
		}

		
		
	}
	
	
	/*Structure for a page
	 * Table_id,Begin_Record Id,End_Record,Id, Data
	 */
	

	private void read_file_into_pages(String table_name) {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(PATH_FOR_DATA + "/" + table_name +".csv"));
			String current_line = br.readLine();
			String meta_info = null;
			String page_data = new String("");
			int bytes_count = 0;
			int record_id_counter= 0;
			Page page = null;
			int begin_id,end_id;
			//String[] splited = null;
			//splited = current_line.trim().split("\\s*,\\s*");
			//begin_id = end_id =  Integer.parseInt(splited[0]);
			begin_id = end_id = 0;
			while(current_line != null)
			{
				bytes_count += current_line.length();
				
				if(bytes_count >= PAGESIZE - table_name.length() - 5) // A Page will have PageSize - Table Length - 3 commas (3 Bytes) - 2 Bytes for Begin and End ids
				{
					page_data  = table_name + "," +  String.valueOf(begin_id) + "," + String.valueOf(end_id)+"," + page_data;
					page = new Page(page_num,page_data);
					disk.add(page);
					page_data = new String("");
					bytes_count = 0;
					++page_num;
					//Uncomment This when required !!
					//splited = current_line.trim().split("\\s*,\\s*");
					//begin_id = Integer.parseInt(splited[0]);
					//end_id = Integer.parseInt(splited[0]);
					begin_id = record_id_counter;
					end_id = record_id_counter;
					continue;						
				}
				
				else //bytes_count < PageSize
				{
					
					page_data += current_line + ","; // Assuming records in the CSV File DO NOT TERMINATE IN A COMMA
					
					//splited = current_line.trim().split("\\s*,\\s*");
					//end_id = Integer.parseInt(splited[0]);
					end_id = record_id_counter;
					record_id_counter++;
				}
				
				current_line = br.readLine();
			}
			
			
			page_data  = table_name + "," +  String.valueOf(begin_id) + "," + String.valueOf(end_id)+"," + page_data;
			page = new Page(page_num,page_data);
			disk.add(page);
			++page_num;
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public int check_disk(String record_table,int id)
	{
		int begin_id;
		int end_id;
		String splited [];
		String page_table;
		String page_contents;
		int page_id;
		
		Iterator it = disk.iterator();
		
		 while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        
		        page_id = (int)pair.getKey();
		        page_contents = pair.getValue().toString();
		        splited = page_contents.trim().split("\\s*,\\s*");
				page_table = splited[0];
				begin_id = Integer.parseInt(splited[1]);
				end_id = Integer.parseInt(splited[2]);
				
				
				if((id > begin_id) && (id < end_id) && (page_table.equalsIgnoreCase(record_table)))
				 	return page_id;	        
		        
		    }

		return -1;
	}
	
	public void show_disks()
	{
		Iterator it = disk.iterator();
		Page P= null;
		while(it.hasNext())
		{
			P = (Page)it.next();
			System.out.println("Page id:" + P.page_id);
			System.out.println("Contents:" + P.page_contents + "\n");
		}
	}
	
	
	private void create_system_catalogue(HashMap config) {
		Iterator it = config.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        Object o = pair.getValue();
	        
	        if(o instanceof HashMap)
	        {
	        	catalogue.put((String) pair.getKey(), new Table((String)pair.getKey(),(HashMap)pair.getValue()));
	        }
	        it.remove(); // avoids a ConcurrentModificationException
	    }
	    
	    display_catalogue();
		
		
	}

	//Testing Function
	void display_catalogue()
	{
		
		Iterator it = catalogue.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue().toString());
	    }
		
	}

	public String getRecord(String tableName, int recordId) {
		return "record";
	}
	
	public static void main(String [] args)
	{
		DBSystem obj = new DBSystem();
		obj.readConfig("test_config");
		obj.populatePageInfo();
		obj.show_disks();
	}
}