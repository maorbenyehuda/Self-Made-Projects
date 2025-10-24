package edu.univ.haifa.bigdata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CorrelationReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    int totalUsage = 0;
	    int totalNotifications = 0;

	    for (Text value : values) {
	        String[] parts = value.toString().split("_");

	        // וידוא שהפורמט תקין
	        if (parts.length == 2) {
	            try {
	                totalUsage += Integer.parseInt(parts[0].trim());
	                totalNotifications += Integer.parseInt(parts[1].trim());
	            } catch (NumberFormatException e) {
	                System.err.println("Error parsing value: " + value.toString());
	                // המשך לולאה במקרה של שגיאה
	            }
	        } else {
	            System.err.println("Invalid input format: " + value.toString());
	        }
	    }

	    context.write(key, new Text("Usage: " + totalUsage + ", Notifications: " + totalNotifications));
	}
}