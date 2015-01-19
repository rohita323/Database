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



public class Select {
	
	private String[] Conditions;
	
	public void getInfo(){
		
		Conditions[0]= "*";
		Conditions[1]= "countries";
		Conditions[2]= "ID > 50";
		Conditions[3]= "CONTINENT = AF";
		Conditions[6]= "ID";
		Conditions[7]= "NAME";
	}
	
	public void search(){
		//takes page, and searches for matched conditions
		//need to check if and or or
	}
	
	

}
