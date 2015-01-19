import java.util.BitSet;


public class Page {
	public int page_id;
	public int begin_id,end_id;
	public String page_contents;
	
	
	Page(int id, String contents)
	{
		page_id = id;
		page_contents = contents;		
	}
}
