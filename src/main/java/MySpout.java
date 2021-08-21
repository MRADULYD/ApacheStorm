import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MySpout extends BaseRichSpout {

    SpoutOutputCollector collector;
    private int checkpointBatchingCount = 0;
    private PartitionContext context;
    private Throwable error;
    String consumerGroupName = "$Default";
    String namespaceName = "Your_EventHub_NameSpace_Name";
    String eventHubName = "EventHubName";
    String sasKeyName = "Your_SasKey_Name_Ex-RootManageSharedAccessPolicy";
    String sasKey = "Your_SasKey;EntityPath=EventHubName";
    String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=Your_AccountName;AccountKey=Your_Account_Key;EndpointSuffix=core.windows.net";
    String storageContainerName = "Your_Blob_Storage_Container_Name";
    String hostNamePrefix = "PUNITP347085L";

    ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder()
            .setNamespaceName("Your_EventHub_NameSpace_Name")
            .setEventHubName("EventHubName")
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey);

    EventProcessorHost host = new EventProcessorHost(
            EventProcessorHost.createHostName(hostNamePrefix),
            eventHubName,
            consumerGroupName,
            eventHubConnectionString.toString(),
            storageConnectionString,
            storageContainerName);

    EventProcessorOptions options = new EventProcessorOptions();

    private Integer i = 0;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        this.collector.emit(new Values(this.i));
        this.i = this.i + 1;
        options.setExceptionNotification(new ErrorNotificationHandler());
        IEventProcessor eventProcessor = new IEventProcessor() {
            @Override
            public void onOpen(PartitionContext context) throws Exception {
                System.out.println("SAMPLE: Partition " + context.getPartitionId() + " is opening");
            }

            // OnClose is called when an event processor instance is being shut down.
            @Override
            public void onClose(PartitionContext context, CloseReason reason) throws Exception {
                System.out.println("SAMPLE: Partition " + context.getPartitionId() + " is closing for reason " + reason.toString());
            }

            // onError is called when an error occurs in EventProcessorHost code that is tied to this partition, such as a receiver failure.
            @Override
            public void onError(PartitionContext context, Throwable error) {
                System.out.println("SAMPLE: Partition " + context.getPartitionId() + " onError: " + error.toString());
            }

            // onEvents is called when events are received on this partition of the Event Hub.
            @Override
            public void onEvents(PartitionContext context, Iterable<EventData> events) throws Exception {
                System.out.println("SAMPLE: Partition " + context.getPartitionId() + " got event batch");
                int eventCount = 0;
                for (EventData data : events) {
                    try {
                        System.out.println("SAMPLE (" + context.getPartitionId() + "," + data.getSystemProperties().getOffset() + "," +
                                data.getSystemProperties().getSequenceNumber() + "): " + new String(data.getBytes(), "UTF8"));

                        eventCount++;

                        // Checkpointing persists the current position in the event stream for this partition and means that the next
                        // time any host opens an event processor on this event hub+consumer group+partition combination, it will start
                        // receiving at the event after this one.
                        checkpointBatchingCount++;
                        if ((checkpointBatchingCount % 1) == 0) {
                            System.out.println("SAMPLE: Partition " + context.getPartitionId() + " checkpointing at " +
                                    data.getSystemProperties().getOffset() + "," + data.getSystemProperties().getSequenceNumber());
                            // Checkpoints are created asynchronously. It is important to wait for the result of checkpointing
                            // before exiting onEvents or before creating the next checkpoint, to detect errors and to ensure proper ordering.
                            context.checkpoint(data).get();
                        }
                    } catch (Exception e) {
                        System.out.println("Processing failed for an event: " + e.toString());
                    }
                }
            }
        };
        try {
            host.registerEventProcessor(eventProcessor.getClass(), options)
                    .whenComplete((unused, e) -> {
                        if (e != null) {
                            System.out.println("Failure while registering: " + e.toString());
                            if(e.getCause() != null){
                                System.out.println("Inner Exception: " + e.getCause().toString());
                            }
                        }
                    })
                    .thenAccept((unused) ->
                    {
                        System.out.println("Press enter to stop." + unused.toString());
                        try
                        {
                            System.in.read();
                        }
                        catch (Exception e)
                        {
                            System.out.println("Keyboard read failed: " + e.toString());
                        }
                    })
                    .thenCompose((unused) ->
                    {
                        return host.unregisterEventProcessor();
                    })
                    .exceptionally((e) ->
                    {
                        System.out.println("Failure while unregistering: " + e.toString());
                        if (e.getCause() != null)
                        {
                            System.out.println("Inner exception: " + e.getCause().toString());
                        }
                        return null;
                    })
                    .get(); // Wait for everything to finish before exiting main!
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field"));
    }
}