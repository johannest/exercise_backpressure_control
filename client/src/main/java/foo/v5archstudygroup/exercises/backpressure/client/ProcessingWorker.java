package foo.v5archstudygroup.exercises.backpressure.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for sending the messages to the server. You are allowed to change this class in any
 * way you like as long as all messages are delivered successfully to the server.
 */
public class ProcessingWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingWorker.class);
    private final ProcessingRequestGenerator requestGenerator;
    private final RestClient client;
    private int sent = 0;
    private int errors = 0;

    public ProcessingWorker(ProcessingRequestGenerator requestGenerator, RestClient client) {
        this.requestGenerator = requestGenerator;
        this.client = client;
    }

    public void run() {
        while (requestGenerator.hasNext()) {
            var message = requestGenerator.next();
            try {
                sent++;
                Integer response = Integer.parseInt(client.sendToServer(message));
                if (response > 1) {
                    System.out.println("Sleeping 200ms");
                    Thread.sleep(200);
                }
                if (response > 4) {
                    System.out.println("Sleeping 2s");
                    Thread.sleep(2000);
                }
                if (response>8) {
                    System.out.println("Sleeping 5s");
                    Thread.sleep(5000);
                }
            } catch (Exception ex) {
                LOGGER.error("Error sending request {}: {}", message.getUuid(), ex.getMessage());
                errors++;
            }
        }
        LOGGER.info("Finished sending {} requests with {} errors", sent, errors);
    }
}
