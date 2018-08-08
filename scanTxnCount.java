import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class scanTxnCount {

    public static void main(String[] args) throws Exception {

		//Instantiating Configuration class
        HBaseConfiguration conf = new HBaseConfiguration();
        
		//Instantiating HTable class		
        HTable htable = new HTable(conf, "TRANSACTIONS");
		
		//Instantiating the Scan class
        Scan scan = new Scan();
        
		//Getting the scan result by performing a table scan
		ResultScanner scanner = htable.getScanner(scan);
        Result r;
		
		//Iterating through each record using a while Loop
        while (((r = scanner.next()) != null)) {
			
            //Assign row values in variable userId
            String userId = Bytes.toString(r.getRow()); 
			//Assign column username values in name
    	    String name = Bytes.toString(r.getValue("details".getBytes(),"name".getBytes()));
			//Assign column txn_count values in count
            String count = Bytes.toString(r.getValue(Bytes.toBytes("details"), Bytes.toBytes("txn_count")));


            System.out.println("RowKey: " + userId+ ", User Name: "+name+",  Count: " + count);
        }
		//closing the scanner
        scanner.close();
        htable.close();
    }
}