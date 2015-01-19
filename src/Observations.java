import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;


public class Observations {
	
	public boolean modify_config_file()
	{
		File inputFile = new File("config.txt");
		File tempFile = new File("myTempFile.txt");

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(inputFile));
		} catch (FileNotFoundException e1) {
			
			e1.printStackTrace();
		}
		BufferedWriter writer=null;
		try {
			writer = new BufferedWriter(new FileWriter(tempFile,true));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		
		String currentLine;

		try {
			while((currentLine = reader.readLine()) != null) {
			    // trim newline when comparing with lineToRemove
			    String trimmedLine = currentLine.trim().split(" ")[0];
			    
			    if(trimmedLine.equals("PAGESIZE")) 
			     {
			    	int pages = 0;
			    	pages = Integer.parseInt(currentLine.trim().split(" ")[1].trim());
			    	currentLine = "PAGESIZE "+ String.valueOf(2*pages);
			     }
			    writer.write(currentLine+"\n");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		boolean successful = tempFile.renameTo(inputFile);
		
		try {
			reader.close();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(successful)
			return true;
		else
			return false;
		
	}
	
	private void runSmallTest() {
		DBSystem dbs = new DBSystem("config.txt");
		for(int i=0;i<5;i++)
		{dbs.getRecord("countries",0);
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
		}		
	}

	public static void main(String [] s)
	{
		Observations obs = new Observations();
		boolean cond = true;
		while(cond==true)
		{
			//obs.runSmallTest();
			obs.runLargeTest();
			float ratio = (float)LRU.hit/(LRU.hit+LRU.miss);
			
			DBSystem.hit_miss.add(ratio);
			if(ratio >= 0.9 - (float)DBSystem.NUM_PAGES/(LRU.hit+LRU.miss))
				cond = false;
			if (!obs.modify_config_file())
				System.out.println("Game Over");
			
		}
		System.out.println(DBSystem.hit_miss.size());
		Iterator it = DBSystem.hit_miss.iterator();
		while(it.hasNext())
		{
			System.out.println(it.next());
		}
		//100 ops
		
		//obs.runLargeTest();
		
	}

	private void runLargeTest() {
		//1000 operations
		DBSystem dbs = new DBSystem("config.txt");
		for(int i=0;i<20;i++)
		{dbs.getRecord("countries",0);
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
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",222);
		dbs.getRecord("countries",145);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",100);
		dbs.getRecord("countries",455);
		dbs.getRecord("countries",499);
		dbs.getRecord("countries",123);
		dbs.getRecord("countries",234);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",41);
		dbs.getRecord("countries",98);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",103);
		dbs.getRecord("countries",110);
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
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",222);
		dbs.getRecord("countries",145);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",100);
		dbs.getRecord("countries",455);
		dbs.getRecord("countries",499);
		dbs.getRecord("countries",123);
		dbs.getRecord("countries",234);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",320);
		dbs.getRecord("countries",120);
		dbs.getRecord("countries",475);
		dbs.getRecord("countries",349);
		dbs.getRecord("countries",163);
		dbs.getRecord("countries",274);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
		dbs.getRecord("countries",107);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",41);
		dbs.getRecord("countries",98);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",53);
		dbs.getRecord("countries",10);
		dbs.getRecord("countries",155);
		dbs.getRecord("countries",199);
		dbs.getRecord("countries",223);
		dbs.getRecord("countries",434);
		dbs.getRecord("countries",335);
		dbs.getRecord("countries",20);
		dbs.getRecord("countries",520);
		dbs.getRecord("countries",275);
		dbs.getRecord("countries",1);
		dbs.getRecord("countries",53);
		dbs.getRecord("countries",74);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
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
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",222);
		dbs.getRecord("countries",145);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",100);
		dbs.getRecord("countries",455);
		dbs.getRecord("countries",499);
		dbs.getRecord("countries",123);
		dbs.getRecord("countries",234);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",41);
		dbs.getRecord("countries",98);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",103);
		dbs.getRecord("countries",110);
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
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",222);
		dbs.getRecord("countries",145);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",100);
		dbs.getRecord("countries",455);
		dbs.getRecord("countries",499);
		dbs.getRecord("countries",123);
		dbs.getRecord("countries",234);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",320);
		dbs.getRecord("countries",120);
		dbs.getRecord("countries",475);
		dbs.getRecord("countries",349);
		dbs.getRecord("countries",163);
		dbs.getRecord("countries",274);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
		dbs.getRecord("countries",107);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",41);
		dbs.getRecord("countries",98);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",53);
		dbs.getRecord("countries",10);
		dbs.getRecord("countries",155);
		dbs.getRecord("countries",199);
		dbs.getRecord("countries",223);
		dbs.getRecord("countries",434);
		dbs.getRecord("countries",335);
		dbs.getRecord("countries",20);
		dbs.getRecord("countries",520);
		dbs.getRecord("countries",275);
		dbs.getRecord("countries",1);
		dbs.getRecord("countries",53);
		dbs.getRecord("countries",74);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);

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
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",222);
		dbs.getRecord("countries",145);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",100);
		dbs.getRecord("countries",455);
		dbs.getRecord("countries",499);
		dbs.getRecord("countries",123);
		dbs.getRecord("countries",234);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",41);
		dbs.getRecord("countries",98);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",103);
		dbs.getRecord("countries",110);
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
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",222);
		dbs.getRecord("countries",145);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",100);
		dbs.getRecord("countries",455);
		dbs.getRecord("countries",499);
		dbs.getRecord("countries",123);
		dbs.getRecord("countries",234);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",320);
		dbs.getRecord("countries",120);
		dbs.getRecord("countries",475);
		dbs.getRecord("countries",349);
		dbs.getRecord("countries",163);
		dbs.getRecord("countries",274);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
		dbs.getRecord("countries",107);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",41);
		dbs.getRecord("countries",98);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",53);
		dbs.getRecord("countries",10);
		dbs.getRecord("countries",155);
		dbs.getRecord("countries",199);
		dbs.getRecord("countries",223);
		dbs.getRecord("countries",434);
		dbs.getRecord("countries",335);
		dbs.getRecord("countries",20);
		dbs.getRecord("countries",520);
		dbs.getRecord("countries",275);
		dbs.getRecord("countries",1);
		dbs.getRecord("countries",53);
		dbs.getRecord("countries",74);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
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
		dbs.getRecord("countries",0);
		dbs.getRecord("countries",1);
		dbs.getRecord("countries",2);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",2000);
		dbs.getRecord("countries",1902);
		dbs.getRecord("countries",1003);
		dbs.getRecord("countries",2001);
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
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",222);
		dbs.getRecord("countries",145);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",100);
		dbs.getRecord("countries",455);
		dbs.getRecord("countries",499);
		dbs.getRecord("countries",123);
		dbs.getRecord("countries",234);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",17);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",41);
		dbs.getRecord("countries",98);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",103);
		dbs.getRecord("countries",110);
		dbs.getRecord("countries",0);
		dbs.getRecord("countries",1);
		dbs.getRecord("countries",2);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",24);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",30);
		dbs.getRecord("countries",741);
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
		dbs.getRecord("countries",300);
		dbs.getRecord("countries",222);
		dbs.getRecord("countries",145);
		dbs.insertRecord("countries", "xx12,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",200);
		dbs.getRecord("countries",100);
		dbs.getRecord("countries",455);
		dbs.getRecord("countries",499);
		dbs.getRecord("countries",123);
		dbs.getRecord("countries",234);
		dbs.getRecord("countries",235);
		dbs.getRecord("countries",1320);
		dbs.getRecord("countries",120);
		dbs.getRecord("countries",1475);
		dbs.getRecord("countries",349);
		dbs.getRecord("countries",163);
		dbs.getRecord("countries",674);
		dbs.getRecord("countries",735);
		dbs.getRecord("countries",187);
		dbs.getRecord("countries",1007);
		dbs.getRecord("countries",27);
		dbs.getRecord("countries",1130);
		dbs.getRecord("countries",141);
		dbs.getRecord("countries",198);
		dbs.insertRecord("countries", "xx13,ZZ,Unknown or unassigned country,AF");
		dbs.getRecord("countries",153);
		dbs.getRecord("countries",110);
		dbs.getRecord("countries",1155);
		dbs.getRecord("countries",1199);
		dbs.getRecord("countries",1223);
		dbs.getRecord("countries",1434);
		dbs.getRecord("countries",1335);
		dbs.getRecord("countries",120);
		dbs.getRecord("countries",1520);
		dbs.getRecord("countries",1275);
		dbs.getRecord("countries",11);
		dbs.getRecord("countries",153);
		dbs.getRecord("countries",174);
		dbs.getRecord("countries",1235);
		dbs.getRecord("countries",1117);

		}		
		}		
		
		
	}


	

