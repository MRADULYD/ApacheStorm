import com.microsoft.azure.eventprocessorhost.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class MyTopology {
    public static void main(String[] args) throws Exception {

        Config config = new Config();
        config.setDebug(true);

        StormTopology myTopology;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("My-Spout",new MySpout());
        builder.setBolt("My-Bolt", new MyBolt()).shuffleGrouping("My-Spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("MyTopology",config,builder.createTopology());
    }
}

