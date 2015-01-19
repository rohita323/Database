import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;
import org.eclipse.datatools.modelbase.sql.query.QueryStatement;
import org.eclipse.datatools.modelbase.sql.query.TableInDatabase;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionColumn;
import org.eclipse.datatools.modelbase.sql.query.helper.StatementHelper;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParseErrorInfo;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserException;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserInternalException;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParseResult;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManager;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManagerProvider;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;




public class DBSystem {
	
	
	public static int PAGESIZE;
	public static int NUM_PAGES;
	public static String PATH_FOR_DATA;
	private List<String> table_names;
	public static List<Page> disk;
	public static int page_num;
	public static HashMap<String, Table> catalogue ;
	public static LRU main_memory;
	public static HashMap config;
	public String config_file;
	static QueryStatement resultObject;
	static SQLQueryParseResult parseResult;
	public static List<Float> hit_miss=new ArrayList<Float>();
	
	
	
	
	DBSystem(String file)
	{
		page_num = 0;
		disk = new ArrayList<Page>();
		catalogue = new HashMap<String,Table>();
		config_file = file;
		init_DB(file);
		
		//start_DB();
	}
	
	
	
	public void readConfig(String configFilePath) {
		
		
		table_names = new ArrayList<String>();
		
		/* Hashmap which has the configurations - 
		 	1.Path_for_data
		 	2.Page Size
		 	3.Num_Pages
		 	4.Hashmaps containing Table meta data with key as Table Name and 
		 	  a Hashmap as value		 	  
		*/ 
		 config = new HashMap();
		 config.put("FileName", configFilePath);
		 
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
			
			begin_id = end_id = 0;
			while(current_line != null)
			{
				bytes_count += current_line.length() + String.valueOf(record_id_counter).length() ;
				
				String page_meta_string = table_name + "," +  String.valueOf(begin_id) + "," + String.valueOf(end_id)+"\n";
				if(bytes_count >= PAGESIZE - page_meta_string.length()) // A Page will have PageSize - Table Length - 3 commas (3 Bytes) - 2 Bytes for Begin and End ids
				{
					page_data  = table_name + "," +  String.valueOf(begin_id) + "," + String.valueOf(end_id)+"\n" +  page_data;
					page = new Page(page_num,page_data);
					disk.add(page);
					page_data = new String("");
					bytes_count = 0;
					++page_num;
			
					begin_id = record_id_counter;
					end_id = record_id_counter;
					continue;						
				}
				
				else //bytes_count < PageSize
				{
					
					page_data += record_id_counter+ ","+ current_line + "\n"; // Assuming records in the CSV File DO NOT TERMINATE IN A COMMA
					
					//splited = current_line.trim().split("\\s*,\\s*");
					//end_id = Integer.parseInt(splited[0]);
					end_id = record_id_counter;
					record_id_counter++;
				}
				
				current_line = br.readLine();
			}
			
			
			page_data  = table_name + "," +  String.valueOf(begin_id) + "," + String.valueOf(end_id)+"\n" + page_data;
			page = new Page(page_num,page_data);
			disk.add(page);
			++page_num;
			catalogue.get(table_name).num_records = record_id_counter;
			catalogue.get(table_name).old_num_of_records = record_id_counter;
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public static Page check_disk(String record_table,int id)
	{
		int begin_id;
		int end_id;
		String splited[];
		String splited_new_line[] = null;
		String page_table;
		
		String page_contents;
		int page_id;
		
		Iterator it = disk.iterator();
		Page P= null;
		
		  
		
		 while (it.hasNext()) {
			 	P = (Page)it.next();
		        
		        page_id = P.page_id;
		        page_contents = P.page_contents;
		        splited_new_line = page_contents.trim().split("\n");
		        splited = splited_new_line[0].split(",");
		        
				page_table = splited[0];
				begin_id = Integer.parseInt(splited[1]);
				end_id = Integer.parseInt(splited[2]);
								
				if((id >= begin_id) && (id <= end_id) && (page_table.equalsIgnoreCase(record_table)))
				 	return P;						
				 	
		        
		 }	 
		
		return null;
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
		HashMap temp = new HashMap<>(config);
		Iterator it = temp.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        Object o = pair.getValue();
	        String table_name = (String) pair.getKey();
	        if(o instanceof HashMap)
	        {
	        	catalogue.put(table_name, new Table(table_name,(HashMap)pair.getValue()));
	        }
	        it.remove(); // avoids a ConcurrentModificationException
	    }
	    
	    display_catalogue();
		
		
	}

	

	public String getRecord(String tableName, int recordId) {
		 return "Record is : " + main_memory.get_record_from_LRU(tableName, recordId);
	}	
	
	public void insertRecord(String table_name, String record)
	{
		main_memory.insert_record_into_page(table_name, record);
		
	}
	public  void init_LRU()
	{
		main_memory = new LRU(NUM_PAGES);
		
	}
	
	public void test_get()
	{
		System.out.println(this.getRecord("test",0));
		System.out.println(this.getRecord("test",6));
		System.out.println(this.getRecord("test",13));
		System.out.println(this.getRecord("test3",0));
		System.out.println(this.getRecord("test3",6));
		System.out.println(this.getRecord("test3",13));
		System.out.println(this.getRecord("test3",6));
		System.out.println(this.getRecord("test3",13));
		System.out.println(this.getRecord("test3",13));
	}
	
	public void insert_test()
	{
		this.insertRecord("test", "907,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "908,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "909,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "910,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "911,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "912,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "913,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "914,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "915,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "916,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		this.insertRecord("test", "917,2,female,24,1,0,SC/PARIS 2167,27.7208,,C");
		
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
		
		void flushPages()
		{
		/* Since primary and secondary memory are independent, no need to
		flush modified pages immediately, instead this function will be called
		to write modified pages to disk.
		Write modified pages in memory to disk.*/
		main_memory.flush_from_LRU();
		}	
		
		
	public void init_DB(String configFile)
	{
		
		readConfig(configFile);
		/*try {
			Runtime.getRuntime().exec("clear");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		//System.out.println("Initialising DB Instance.");
		//System.out.println("Populating Tables from config file details...");
		populatePageInfo();
		//System.out.println("Initialising Main Memory - LRU..");
		init_LRU();
		//System.out.println("Initialisation complete.Please Enter to start DB.");
		/*Scanner scanner = new Scanner(System.in);
	    String readString = scanner.nextLine();
	    if (readString.equals(""))
	    	return;*/
	    
	}
	
	public void start_DB()
	{
		/*try {
			Runtime.getRuntime().exec("clear");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		System.out.println("Choose your option.");
		System.out.println("1.Get Record with record id(Line Number)");
		System.out.println("2.Insert Record");
		System.out.println("3.Flush Pages on to Disk");
		System.out.println("4.Run Dummy Operations");
		System.out.println("0.Exit");
		
		int n=-1;
		
		while(true)
		{
			System.out.println("\n\n Enter you choice.");
			Scanner in = new Scanner(System.in);
			n = in.nextInt();
			/*try {
				Runtime.getRuntime().exec("clear");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			
			switch(n)
			{
			case 1 : System.out.println("Enter Record id");
					 int recordId = in.nextInt();
					 System.out.println("Enter Table Name");
					 String tableName = in.next(); 
					 
					 System.out.println(getRecord(tableName, recordId));
					 break;
			case 2 : System.out.println("Enter Table Name");
					 tableName = in.next(); 
					 System.out.println("Enter Record");
					 String record = in.next();
					 break;
			case 3 : System.out.println("Press Y to flush pages.N to go back.");
					 String response = in.next();
					 if(response.equalsIgnoreCase("Y"))
						 flushPages();
			 		 break;
			case 4 : System.out.println("Press Y to flush pages.N to go back.");
					 response = in.next();
					 if(response.equalsIgnoreCase("Y"))
						 start_dummy_ops();
			case 0 : System.out.println("Press Y to EXIT.N to go back to menu.");
					 response = in.next();
					 if(response.equalsIgnoreCase("Y"))
						 return;
			default :System.out.println("You entered a wrong choice. Please choose again.");
					  break;
			
			}
		}
		
		
		
	}
	
	private void start_dummy_ops() {
		System.out.println("Operations of Getting Data");
		for(int i=0;i<13;i+=6)
		{
			getRecord("test", i);
		}
		for(int i=0;i<13;i+=6)
		{
			getRecord("test", i);
		}
		getRecord("test", 12);
		
		System.out.println("Operations of Inserting data");
		insert_test();
		
		System.out.println("Operations of Getting Data from inserted data");
		getRecord("test", 15);
		getRecord("test", 17);
		
		System.out.println("Operation of Flushing");
		flushPages();
		
		
		
			
		
	}

	public static void TestCase()
	{
		DBSystem dbs = new DBSystem("config.txt");
		
		
		
		dbs.getRecord("countries",0);
		dbs.getRecord("countries",1);
		dbs.getRecord("countries",2);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",2);
		dbs.getRecord("countries",2);
		dbs.getRecord("countries",3);
		dbs.getRecord("countries",41);
		dbs.getRecord("countries",9);
		dbs.getRecord("countries",39);
		dbs.getRecord("countries",28);
		dbs.getRecord("countries",1);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",38);
		dbs.getRecord("countries",39);
		dbs.getRecord("countries",31);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",42);
		dbs.getRecord("countries",28);
		dbs.getRecord("countries",1);
		dbs.getRecord("countries",15);
		dbs.getRecord("countries",23);

	}

	
	
	
	/******************* 
	 * 
	 * Deliverable 2 Begins 
	 * 
	 *******************/
	
	void queryType(String query) { 
		/* 
		Determine the type of the query (select/create) and 
		invoke appropriate method for it. 
		*/
		String phrase = query.substring(0,query.indexOf(" "));
		
		if(phrase.equalsIgnoreCase("SELECT"))
			selectCommand(query);
		
		else if(phrase.equalsIgnoreCase("CREATE"))
		{
			System.out.println("Phrase: "+ phrase);
			createCommand(query);
		}
		
		else
			System.out.println("Inalid Syntax- Only Select/Create Permitted");
		
		
		}
		void createCommand(String query) { 
		/* 
		Use any SQL parser to parse the input query. Check if the table doesn't exists
		and execute the query. 
		The execution of the query creates two files : <tablename>.data and 
		<tablename>.csv. An entry should be made in the system config file used 
		in previous deliverable. 
		Print the query tokens as specified at the end. 
		**format for the file is given below 
		*/ 
			 SQLParser parser = new SQLParser();
		        
		        StatementNode stmt;
				try {
					stmt = parser.parseStatement(query);
				
		     // Create a stream to hold the output
		        ByteArrayOutputStream baos = new ByteArrayOutputStream();
		        PrintStream ps = new PrintStream(baos);
		        // IMPORTANT: Save the old System.out!
		        PrintStream old = System.out;
		        // Tell Java to use your special stream
		        System.setOut(ps);
		        // Print some output: goes to your special stream
		        stmt.treePrint();
		        // Put things back
		        System.out.flush();
		        System.setOut(old);
		        // Show what happened
		        String tree = baos.toString();
		        tree=tree.replace("[", "");
				tree=tree.replace("]", "");
				String [] new_line = tree.split("\n");
				String temp_object = "";
				for(int i=0;i<new_line.length;i++)
					if(new_line[i].trim().contains(":"))
						temp_object += new_line[i].trim()+"\n";
				new_line = temp_object.split("\n");
				//System.out.println(temp_object);
				String table_name = new_line[0].split(":")[1].trim();
				HashMap<String, String> atts = new HashMap<String, String>();
				String [] temp;
				int c =0;
				for(int i=5;i<new_line.length;i++)
				{
					temp = new_line[i].split(":");
					if(temp[0].length()==1 && temp[0].substring(0,1).matches("[0-9]"))
					{
						
						atts.put(new_line[i+2].split(":")[1].trim(), new_line[i+1].split(":")[1].trim());
						c++;
						i+=3;
					}
				}
				
				catalogue.put(table_name, new Table(table_name, atts));
			    
				FileWriter fStream;
		        try {
		            fStream = new FileWriter(PATH_FOR_DATA + "/" + table_name +".csv", true);
		            fStream.close();
		        } catch (IOException ex) {
		            System.out.println("Error in Creating CSV File");
		        }
				
		        try {
		            fStream = new FileWriter(new File(PATH_FOR_DATA + "\\" + table_name +".data"),true);
		            String att_details = "";
		            List details = catalogue.get(table_name).atts_details;
		            Iterator it = details.iterator();
		            String att_name_type;
		            while(it.hasNext())
		            {
		            	temp = (String[]) it.next();
		            	att_details += temp[0]+":"+temp[1]+",";
		            }
		            fStream.append(att_details);
		            fStream.close();
		            System.out.println("Querytype: Create");
		            System.out.println("Tablename: "+table_name);
		            System.out.println("Attributes: "+att_details);
		        } catch (IOException ex) {
		            System.out.println("Error in Creating Data File");
		            
		        }
		        
		        try {
		            fStream = new FileWriter(new File(PATH_FOR_DATA + "\\" + config_file),true);		            
		            fStream.append("\nBEGIN\n");
		            fStream.append(table_name+"\n");
		            String att_details = "";
		            List details = catalogue.get(table_name).atts_details;
		            Iterator it = details.iterator();
		            
		            while(it.hasNext())
		            {
		            	temp = (String[]) it.next();
		            	att_details += temp[0]+", "+temp[1]+"\n";
		            }
		            fStream.append(att_details);
		            fStream.append("END");
		            fStream.close();
		            
		        } catch (IOException ex) {
		            System.out.println(ex.getLocalizedMessage());
		        }	
				
	   } catch (StandardException e) {
					// TODO Auto-generated catch block
					System.out.println("Invalid Query");
				}
				
		}
		
			
	private static String readFile(String path) throws IOException {
		FileInputStream stream = new FileInputStream(new File(path));
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
					fc.size());
			/* Instead of using default, pass in a decoder. */
			return Charset.defaultCharset().decode(bb).toString();
		} finally {
			stream.close();
		}
	}

	
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void selectCommand(String sql){
		try {
			SQLQueryParserManager parserManager = SQLQueryParserManagerProvider
					.getInstance().getParserManager("mySQL", "v5.5");
			
			String broken[] = sql.trim().toUpperCase().split(" ");
				
			parseResult = parserManager.parseQuery(sql);
			
			resultObject = parseResult.getQueryStatement();

			String parsedSQL = resultObject.getSQL();
//			System.out.println(parsedSQL);
			
			selectCommand2(parsedSQL);
						

		} catch (SQLParserException spe) {
			System.out.println(spe.getMessage());
			List syntacticErrors = spe.getErrorInfoList();
			Iterator itr = syntacticErrors.iterator();
			while (itr.hasNext()) {
				SQLParseErrorInfo errorInfo = (SQLParseErrorInfo) itr.next();
				String errorMessage = errorInfo.getParserErrorMessage();
				int errorLine = errorInfo.getLineNumberStart();
				int errorColumn = errorInfo.getColumnNumberStart();
			}

		} catch (SQLParserInternalException spie) {
			System.out.println(spie.getMessage());
		}
	}
	

	static void selectCommand2(String query) { 
		
		System.out.println("Querytype: SELECT");
		int flag = Table(query);

		if(flag == 1)
		{
			Distinct(query);
			Rest(query);
		}
		else
		{
			System.out.println("Invalid Query");
		}
	}
	
	static int Table(String query){

		int flag=0;
		String columns = "";

		List tableList = StatementHelper.getTablesForStatement(resultObject);
		System.out.print("Tablename: ");
		
		for (Object obj : tableList) {
			flag=0;
			TableInDatabase t = (TableInDatabase) obj;
			
			Iterator it = DBSystem.catalogue.entrySet().iterator();
	    		while (it.hasNext()) {
	        		Map.Entry pair = (Map.Entry)it.next();
	        		if(t.getName().toLowerCase().equals((String) pair.getKey()))
					flag=1;
	        	}
			
			List details = 	catalogue.get(t.getName().toLowerCase()).atts_details;
		    for (int i=0; i<details.size() ; i++)
		            {
			    		   	if(query.toUpperCase().contains(((String [])details.get(i))[0].toUpperCase()))
		            		{columns += ((String [])details.get(i))[0].toUpperCase() + " ";}
		            }			
			
			if(flag==1) 
				System.out.print(t.getName() + ",");
			else
				return 0;
		}
		
		System.out.print("\nColumns: " + columns + "\n");
	
        	return flag;
	}
	static void Distinct(String query)
	{
		String REGEX = "DISTINCT\\((.*)\\)";
		Pattern pattern = Pattern.compile(REGEX);
		Matcher matcher = pattern.matcher(query.replaceAll("\\s+",""));
		if(matcher.find())
		{	System.out.println("Distinct: " + matcher.group(1));		}
		else
		{	System.out.println("Distinct: N/A");		}
	}
	

	static void Rest(String query)
	{
		String Clauses[]=new String[4];
		Clauses[0]=Clauses[1]=Clauses[2]=Clauses[3]="N/A";
		
		String splint[] = query.split("\n");
		
		
		for(int i = 0; i < splint.length; i++)
		{
			String broken[]=splint[i].trim().split("\\s+");
			String result = "";
			String result2 = "";
			
			for (int j=1; j<broken.length; j++) {
				result += broken[j];
				
				if(j>1)
				{result2+= broken[j];}
			}
			
			if(broken[0].equals("WHERE"))
			{	Clauses[0]= result;		}
			
			if(broken[0].equals("GROUP"))
			{	Clauses[2]= result2;	}
			
			if(broken[0].equals("ORDER"))
			{	Clauses[1]= result2;	}
			
			if(broken[0].equals("HAVING"))
			{	Clauses[3]= result;		}
					
		}
		
		 System.out.println("Condition: " + Clauses[0] + "\nOrder By: " + Clauses[1] + "\nGroup By: " + Clauses[2] + "\nHaving: " +  Clauses[3]);
		
	}
	
	
	public static void main(String [] args) throws IOException 
	{
		DBSystem obj = new DBSystem("config.txt");
		obj.queryType("select distinct (ID, CODE) from countries where ID=CODE group by ID having CODE=50 order by ID;");
		
	}
}
