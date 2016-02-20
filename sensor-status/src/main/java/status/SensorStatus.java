package status;
public class SensorStatus {
	public String from = "";
	public String to = "";
	public String status = "";
	public String today = "";

	public  SensorStatus(String today,String from,String to, String status){
	 	this.from = from;
		this.to = to;
		this.status = status;
		this.today = today;
	}

}