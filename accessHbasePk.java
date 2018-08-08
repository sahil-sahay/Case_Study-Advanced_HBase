import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class accessHbasePk{

   public static void main(String[] args) throws IOException, Exception{
   
      //Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      //Instantiating HTable class
	  HTable table = new HTable(config, "TRANSACTIONS");

      //Instantiating Get class
      Get g = new Get(Bytes.toBytes("108"));
      
      //Reading the data
      Result result = table.get(g);

      //Reading values from Result class object
      byte [] name = result.getValue(Bytes.toBytes("details"),Bytes.toBytes("name"));

      byte [] txn = result.getValue(Bytes.toBytes("details"),Bytes.toBytes("txn_count"));

      //Printing the values
      String user  = Bytes.toString(name);
      String count = Bytes.toString(txn);
      
      System.out.println("Customer Name: " + user + " ,Number of transactions: " + count);
   }
}
