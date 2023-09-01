import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.HashMap;

class EventJava {
    private HashMap<String,Object> attributes;

    public EventJava(String activity){
        this.attributes = new HashMap<>();
        this.attributes.put(EventLogJava.ACTIVITY_NAME,activity);
    }
    public EventJava(HashMap<String,Object> attributes) {
        this.attributes = attributes;
    }

    public HashMap<String,Object> getAttributes() {
        return attributes;
    }
}

class TraceJava {
    private HashMap<String,Object> attributes;
    private ArrayList<EventJava> events;

    public TraceJava(String caseID){
    this(caseID, new ArrayList<>());
    }
    public TraceJava(String caseID, ArrayList<EventJava> events){
        this.attributes = new HashMap<>();
        this.attributes.put(EventLogJava.CASE_ID_NAME,caseID);
        this.events = events;
    }
    public TraceJava(HashMap<String,Object> attributes, ArrayList<EventJava> events) {
        this.attributes = attributes;
        this.events = events;
    }

    public HashMap<String,Object> getAttributes() {
        return attributes;
    }

    public ArrayList<EventJava> getEvents() {
        return events;
    }
}

class EventLogJava {
    public static final String CASE_ID_NAME = "case:concept:name";
    public static final String ACTIVITY_NAME = "concept:name";

    private String logName;
    private ArrayList<TraceJava> traces;

    public EventLogJava(String logName, ArrayList<TraceJava> traces) {
        this.logName = logName;
        this.traces = traces;
    }

    public EventLogJava() {
        int numTraces = 150_500;
        int numEventsPerTrace = 20;
        this.logName = "Test Log";
        this.traces = new ArrayList<>();
        for (int i = 0; i < numTraces; i++) {
            ArrayList<EventJava> events = new ArrayList<>();
            TraceJava trace = new TraceJava("Case " + i, events);
            for (int j = 0; j < numEventsPerTrace; j++) {
                events.add(new EventJava("Activity " + j));
            }
            this.traces.add(trace);
        }
    }

    public ArrayList<TraceJava> getTraces() {
        return this.traces;
    }

    public void setTraces(ArrayList<TraceJava> traces) {
        this.traces = traces;
    }

    public String getLogName() {
        return this.logName;
    }
}



class HelloProcessMining {
    public static void deleteFile(String filePath){
        File file = new File(filePath);
        file.delete();
    }
    private static native String addArtificialActs(String importPath, String exportPath);

    static {
        System.loadLibrary("java_bridge");
    }

    public static void main(String[] args) throws IOException {
        EventLogJava log = new EventLogJava();
        Gson gson = new Gson();
        System.out.println("Before Rust Call");
        String tmpDir = System.getProperty("java.io.tmpdir");

        String importPath = File.createTempFile("process-mining-bridge-import",".json").getAbsolutePath();
        String exportPath = File.createTempFile("process-mining-bridge-export",".json").getAbsolutePath();
        long startTime = System.nanoTime();
        try (Writer writer = new FileWriter(importPath)) {
            gson.toJson(log, writer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try{
            String res = addArtificialActs(importPath,exportPath);
            System.out.println("Res: " + res);
            try (Reader reader = new FileReader(exportPath)) {
                EventLogJava log2 = gson.fromJson(reader,EventLogJava.class);
                System.out.println("Got back Log " + log2.getLogName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }catch (Exception e){
            System.err.println(e);
        } finally {
            deleteFile(importPath);
            deleteFile(exportPath);
        }

//        String res = addArtificialActs(FileSystems.getDefault().getPath("import.json").toAbsolutePath().toString());

//        EventLogJava log2 = gson.fromJson(res,EventLogJava.class);
//        EventLogJava log2 = addArtificialActs(log);
        System.out.println("After Rust Call (took " + ((System.nanoTime() -
                startTime) / 1000000.0) + "ms)");
//
//
//        System.out.println("Before Java Call");
//        startTime = System.nanoTime();
//        for (TraceJava trace : log.getTraces()) {
//            AttributesJava startAttributes = new AttributesJava();
//            startAttributes.setAttribute(EventLogJava.ACTIVITY_NAME, "__START__");
//            EventJava startEvent = new EventJava(startAttributes);
//            trace.getEvents().add(0,startEvent);
//            AttributesJava endAttributes = new AttributesJava();
//            endAttributes.setAttribute(EventLogJava.ACTIVITY_NAME, "__END__");
//            EventJava endEvent = new EventJava(endAttributes);
//            trace.getEvents().add(endEvent);

            // for (EventJava event : trace.getEvents()) {
            //     System.out.println(event.getAttributes().getAttribute(EventLogJava.ACTIVITY_NAME));
            // }
//        }
//        System.out.println("After Java Call (took " + ((System.nanoTime() -
//                startTime) / 1000000.0) + "ms)");
    }

}
