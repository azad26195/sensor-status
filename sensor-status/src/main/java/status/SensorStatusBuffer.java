package status;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.FunctionCall;
import cascading.operation.assertion.AssertExpression;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Date;
import java.util.*;
import java.text.SimpleDateFormat;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.DateTimeComparator;

public class SensorStatusBuffer extends BaseOperation implements Buffer {
	
	public SensorStatusBuffer() {
	    super( 1, new Fields("sensorid","range","status") );
	  }
	
	public SensorStatusBuffer( Fields fieldDeclaration ){
	    super( 1, fieldDeclaration );
	  }
	
	// Converts String to Date
	 public Date toDateString(String timestampString) {
		return new Date(new Long(timestampString.trim() + "000"));
	 }
	 
	 // Converts Timestamp String to DateTime
	 public DateTime toDateTime(String timestampString){
	    Long timestamp = new Long(timestampString.trim() + "000");
	    return new DateTime(timestamp,DateTimeZone.UTC); 
	 }
	 
	 // Converts DateTime to HHMM
	 public String toHHMM(String timestampString){
		 DateTime date = toDateTime(timestampString.trim());
		 return date.hourOfDay().getAsText() + ":" + date.minuteOfHour().getAsText();    // return sdf.format(toDateString(timestampString.trim()));
	}
	 
	//main Buffer operator function. 
	public void operate(FlowProcess flowProcess, BufferCall bufferCall){
		String startTimeString = "";
		String currentTimeString = "";
		String endTimeString = "";
		Long timeStringDifference = 0L;
		Date endTime = new Date();
		Date currentTime = new Date();
		
		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
		TupleEntry currentSensor = new TupleEntry();
		ArrayList<SensorStatus> sensorList = new ArrayList();
		
		currentSensor = arguments.next();
		startTimeString = currentSensor.getString(2);
		currentTimeString = startTimeString;
		
		while(arguments.hasNext()){
			currentSensor = arguments.next();
			endTimeString = currentSensor.getString(2);
			currentTime = toDateString(currentTimeString);
			endTime = toDateString(endTimeString);
			timeStringDifference = endTime.getTime() - currentTime.getTime();
			
			if(timeStringDifference > 3000*60){
				sensorList.add(new SensorStatus(toHHMM(startTimeString), toHHMM(currentTimeString), "ON"));
				sensorList.add(new SensorStatus(toHHMM(currentTimeString), toHHMM(endTimeString), "OFF"));
				startTimeString = endTimeString;
				currentTimeString = startTimeString;
			}
			else{
				currentTimeString = endTimeString;
			}
		}
		
		sensorList.add(new SensorStatus(toHHMM(startTimeString), toHHMM(endTimeString), "ON"));
		Iterator<SensorStatus> resultList = sensorList.iterator();
		
		while(resultList.hasNext()){
			SensorStatus results = resultList.next();
			Tuple result = new Tuple(currentSensor.getString(1),results.from + "-" + results.to, results.status) ;
		    bufferCall.getOutputCollector().add(result);

		}
	}

	

	
	
}
