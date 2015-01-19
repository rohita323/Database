import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
 
public class LRU {

	private int fixed_size;
	private int LRU_length;
 	public static int hit;
 	public static int miss;
	private Node head;
	private Node tail;
	
	
	
	public static HashMap<Integer, Node> LRU_container = new HashMap<Integer, Node>();
	
	public LRU(int fixed_size) {
		LRU_length = 0;
		this.fixed_size = fixed_size;
		hit = miss=0;
	}
	
	class Node {
		public int key;
		public String content;
		
		public int start_id;
		public int end_id;
		
		public Node pre;
		public Node next;
		
		private int modified;

	 
		public Node(int key, String value) {
			content = value;
			this.key = key;
			modified = 0;
		}
	
	}
	
	public String get_record_from_page(int key,int record_id,String table_name) {
		
		if (LRU_container.containsKey(key)) {
			Node found = LRU_container.get(key);
			removeNode(found);
			setHead(found);
			String record = new String("");
			String page_content = found.content;
			String [] splited = null;
			String [] splited_new_line = null;
			splited_new_line = page_content.split("\n");
			for(int i=1;i<splited_new_line.length;i++)
			{
				splited = splited_new_line[i].split(",");
				
				if(Integer.parseInt(splited[0])==record_id)
				{
					
					return new String(splited_new_line[i]);
					
				}
			}			

			
			return "Record Not Found";
		}
		return "LRU Error : Page with Page_ID"+ key + "not found."; 	
		
	}
 
	public int get_page_id(String record_table,int id)
	{
		int begin_id;
		int end_id;
		String splited [];
		String splited_new_line [] ;
		String page_table;
		String page_contents;
		int page_id;
		
		Iterator it = LRU_container.entrySet().iterator();
		
		 while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        
		        page_id = (int)pair.getKey();
		        page_contents = ((Node)pair.getValue()).content.toString();
		        splited_new_line = page_contents.trim().split("\n");
		        splited = splited_new_line[0].split(",");
		        
				page_table = splited[0];
				begin_id = Integer.parseInt(splited[1]);
				end_id = Integer.parseInt(splited[2]);
				
				if((id >= begin_id) && (id <= end_id) && (page_table.equalsIgnoreCase(record_table)))
				 	return page_id;	        
		        
		    }

		return -1;
	}
	
	public int insert_record_into_page(String table_name,String record)
	{
		if(!DBSystem.catalogue.containsKey((String) table_name))
		{
			System.out.print("Table not found");
			return -1;
		}
		int records_in_table = DBSystem.catalogue.get((String)table_name).num_records;
		int page_id = get_page_id(table_name, records_in_table-1);
		
		Page P;
		if(page_id == -1)
		{
			P = DBSystem.check_disk(table_name, records_in_table-1);
			flush_from_LRU();
			P = DBSystem.check_disk(table_name, records_in_table-1);
			
			if(P==null)
			{
				System.out.println("LRU.insert_into_pages:Table Error : Probably LRU is FULL.Flush");
				return -2;
				
			}
		}
		else
		{
			P=new Page(page_id,LRU_container.get(page_id).content);
		}
		String modified_data = P.page_contents + records_in_table +"," +record+"\n";
		if(modified_data.length()<= DBSystem.PAGESIZE)
		{
			
			P.page_contents = P.page_contents + records_in_table +"," +record +"\n" ;
			String [] splited = P.page_contents.split("\n")[0].split(",");
			splited[2] = String.valueOf(records_in_table)+"\n";
			P.page_contents =splited[0]+","+splited[1]+","+splited[2]+ P.page_contents.substring(P.page_contents.indexOf("\n")+1) ;
					
			DBSystem.catalogue.get((String)table_name).num_records +=1;
			int status = insert_into_LRU(P.page_id, P.page_contents, 1);
			if(status!=0)
			{
				return -1;
			}
			
		}
		else
		{
			// Format of New Page is TableName,StartingRecordID,EndRecordId,Records 
			String new_page_content = table_name + "," + records_in_table +"," + records_in_table +  "\n" + records_in_table +"," +record+"\n";
			DBSystem.catalogue.get((String)table_name).num_records+=1;
			P = new Page(DBSystem.page_num,new_page_content);
			int status = insert_into_LRU(P.page_id, P.page_contents, 1);
			if(status!=0)
			{
				return -1;
			}				
		}
		
		return 0;
		
	}
	
	public String get_record_from_LRU(String record_table, int record_id)
	{
		int page_id = get_page_id(record_table,record_id);
		if(page_id != -1)
			{
				System.out.println("Hit");
				hit++;
				return get_record_from_page(page_id,record_id,record_table);
			}
		else
		{
			System.out.println("MISS");
			miss++;
		}
		Page P = DBSystem.check_disk(record_table, record_id);
		 
		if(P==null)
		{
			return "Record with Record ID : " + String.valueOf(record_id) + "doesn't exists.";
			
		}
		
		insert_into_LRU(P.page_id, P.page_contents);
		//System.out.println(LRU_container.get(P.page_id).content);
		//System.out.println(get_record_from_page(P.page_id,record_id,record_table));
		return new String(get_record_from_page(P.page_id,record_id,record_table));
	}
	
	
	public void flush_from_LRU()
	{
		Iterator it = LRU.LRU_container.entrySet().iterator();
		int page_id;
		String page_contents;
		Page p;
		
		 while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        page_id = (int)pair.getKey();
		        page_contents = ((Node)pair.getValue()).content.toString();
		        if(((Node)pair.getValue()).modified==1)
		        {
		        	try{
			        	p = DBSystem.disk.get(page_id);
			        	p.page_contents = page_contents;
			        	((Node)pair.getValue()).modified = 0;
				        DBSystem.disk.add(p);
			        }
			        catch(Exception e)
			        {
			        	p = new Page(page_id,page_contents);
			        	((Node)pair.getValue()).modified = 0;
			        	DBSystem.disk.add(p);
			        	
			        }
			        
			        write_page_to_file(p);			
		 }
		}
		 
	}

	private void write_page_to_file(Page p) {
		
		
		
		 	try{
		 		 
		 		String[] splited_new_line = p.page_contents.split("\n");
		 		String[] page_details = splited_new_line[0].split(",");
		 		Table t = DBSystem.catalogue.get(page_details[0]);		 		
		 		PrintWriter f_out = new PrintWriter(new BufferedWriter(new FileWriter(DBSystem.PATH_FOR_DATA+"/"+page_details[0] + ".csv", true)));
		 		int begin_id = Integer.parseInt(page_details[1]);
		 		int end_id = Integer.parseInt(page_details[2]);
		 		int index_to_start = t.old_num_of_records - begin_id + 1;
		 		index_to_start =1;
		 		for(int i = index_to_start;i<splited_new_line.length;i++)
		 		{
		 			f_out.write("\n"+ splited_new_line[i].substring(splited_new_line[i].indexOf(",")+1 ));
		 		}
		 		
				

		 		f_out.close();
		 	}
		 	catch(ArrayIndexOutOfBoundsException e)
		 	{
		 		
		 	}
		 	catch(IOException e)
		 	{
		 		System.out.print(e.getMessage());
		 	}
			
		
	}

	public int insert_into_LRU(int key, String value,Integer ... b) {
		Integer modified = b.length > 0 ? b[0] : 0;
		if (LRU_container.containsKey(key)) {
			Node found = LRU_container.get(key);
			found.content = value;
			found.modified= modified;
			removeNode(found);
			setHead(found);
			
		} else {
			Node new_key = new Node(key, value);
			new_key.modified = modified;
			if (LRU_length < fixed_size) {
				//System.out.println("MISS "+LRU_length);
				setHead(new_key);
				LRU_container.put(key, new_key);
				LRU_length++;

			} else {
				if(tail.modified==1)
				{
					flush_from_LRU();
				}
				
					LRU_container.remove(tail.key);
					
					tail = tail.pre;
					if (tail != null) {
						tail.next = null;
					}
					++DBSystem.page_num;
					setHead(new_key);
					LRU_container.put(key, new_key);
					
				
			}
		}
		
		return 0;
	}

		
	public void removeNode(Node node) {
		Node cur = node;
		Node pre = cur.pre;
		Node post = cur.next;
 
		if (pre != null) {
			pre.next = post;
		} else {
			head = post;
		}
 
		if (post != null) {
			post.pre = pre;
		} else {
			tail = pre;
		}
	}
 
	public void setHead(Node node) {
		node.next = head;
		node.pre = null;
		if (head != null) {
			head.pre = node;
		}
 
		head = node;
		if (tail == null) {
			tail = node;
		}
	}

	
	
	public static void show_LRU()
	{
		System.out.println("LRU Pages");
		Iterator it = LRU.LRU_container.entrySet().iterator();
		int page_id;
		String page_contents;
		
		
		 while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        
		        page_id = (int)pair.getKey();
		        page_contents = ((Node)pair.getValue()).content.toString();
		        
						
				
				System.out.println("Page id : " + page_id);
				System.out.println("Contents : " + page_contents);
		        
		    }
	}
	
}