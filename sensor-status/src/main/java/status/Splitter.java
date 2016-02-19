package status;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.*;
public class Splitter extends BaseOperation implements Function{
	public Splitter(Fields fieldDeclaration) {
		super(1,fieldDeclaration );
	}
	
	public  void operate( FlowProcess flowProcess, FunctionCall functionCall ) {
		// get the arguments TupleEntry
		TupleEntry arguments = functionCall.getArguments();
		String[] tokens = arguments.getString(0).split(",");
		
	    // create a Tuple to hold our result values
		Tuple result = new Tuple();
		result.add(tokens[0]);
		result.add(tokens[1]);
		result.add(tokens[2]);
		 // return the result Tuple
		functionCall.getOutputCollector().add( result );
	}
	
}