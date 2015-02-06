package my.com.example.pratice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text>
{
	   public void reduce( Text key, Text values, Context context) throws java.io.IOException ,InterruptedException
	   {  Text sentimentIs;
	     String a = values.toString();
	     String[] c = a.split("\\s+");
	    for(int j=0;j<c.length;j++)
	     {
	    	 if(c[j].equals("Positive"))
	    	 {
	    		 sentimentIs.set(c[j]);
	    		 context.write(key,sentimentIs);
	    	 }
	    	 else
	    	 if(c[j].equals("Negative"))
	    	 {
	    		 sentimentIs.set(neg);
	    		 context.write(key, sentimentIs);
	    	 }
	    	 else
	    	 if(c[j].equals("Neutral"))
	    	 {
	    		 sentimentIs.set(neu);
	    		 context.write(key,sentimentIs);
	    	 }
	     
	    
	     }
	     
	   }
	 
}

