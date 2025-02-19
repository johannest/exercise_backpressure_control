package foo.v5archstudygroup.exercises.backpressure.server.controller;

import foo.v5archstudygroup.exercises.backpressure.messages.Messages;
import foo.v5archstudygroup.exercises.backpressure.server.DataProcessor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * This is the REST controller that receives requests from the client and forwards them to the {@link DataProcessor}.
 * Feel free to change this in any way you like.
 */
@Controller
public class DataProcessingController {

    private final DataProcessor dataProcessor;

    public DataProcessingController(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @PostMapping("/process")
    public ResponseEntity<String> process(@RequestBody Messages.ProcessingRequest request) {
        System.out.println(dataProcessor.getCount());
        dataProcessor.enqueue(request);
        return ResponseEntity.accepted().body(String.valueOf(dataProcessor.getCount()));
    }
}
