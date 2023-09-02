import com.google.gson.Gson;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

class EventJava {
    private HashMap<String, String> attributes;
    public EventJava(){

    }
    public EventJava(String activity) {
        this.attributes = new HashMap<>();
        this.attributes.put(EventLogJava.ACTIVITY_NAME, activity);
    }

    public EventJava(HashMap<String, String> attributes) {
        this.attributes = attributes;
    }

    public HashMap<String, String> getAttributes() {
        return attributes;
    }
}

class TraceJava {
    private HashMap<String, String> attributes;
    private ArrayList<EventJava> events;
    public TraceJava(){

    }
    public TraceJava(String caseID) {
        this(caseID, new ArrayList<>());
    }

    public TraceJava(String caseID, ArrayList<EventJava> events) {
        this.attributes = new HashMap<>();
        this.attributes.put(EventLogJava.CASE_ID_NAME, caseID);
        this.events = events;
    }

    public TraceJava(HashMap<String, String> attributes, ArrayList<EventJava> events) {
        this.attributes = attributes;
        this.events = events;
    }

    public HashMap<String, String> getAttributes() {
        return attributes;
    }

    public ArrayList<EventJava> getEvents() {
        return events;
    }
}

class EventLogJava {
    public static final String CASE_ID_NAME = "case:concept:name";
    public static final String ACTIVITY_NAME = "concept:name";

    private HashMap<String, String> attributes;
    private ArrayList<TraceJava> traces;
    public EventLogJava(){

    }
    public EventLogJava(String logName, ArrayList<TraceJava> traces) {
        this.attributes = new HashMap<>();
        this.attributes.put("name", logName);
        this.traces = traces;
    }

    public EventLogJava(int numTraces, int numEventsPerTrace) {
        this.attributes = new HashMap<>();
        this.attributes.put("name", "Test Log");
        System.out.println("Artificial log with " + numTraces + " Traces with " + numEventsPerTrace + " events per trace");
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
    // Time until into/from: 69.67ms

    public ArrayList<TraceJava> getTraces() {
        return this.traces;
    }

    public void setTraces(ArrayList<TraceJava> traces) {
        this.traces = traces;
    }

    public HashMap<String, String> getAttributes() {
        return this.attributes;
    }
}

class Test {
    private ArrayList<String> data = new ArrayList<>(List.of(new String[]{"Hello", "World"}));
}


class HelloProcessMining {
    static Gson gson = new Gson();

    static {
        System.loadLibrary("java_bridge");
    }

    public static void deleteFile(String filePath) {
        File file = new File(filePath);
        file.delete();
    }

    private static native byte[] addArtificialActs(byte[] data);

    private static native byte[] addArtificialActsAvro(byte[] data);

    private static native String addArtificialActsUsingFiles(String importPath, String exportPath);

    public static EventLogJava addArtificialActs(EventLogJava log) {
        byte[] bytes = gson.toJson(log).getBytes();
        bytes = addArtificialActs(bytes);
        return gson.fromJson(new String(bytes), EventLogJava.class);
    }

    public static byte[] serializeWithGson(EventLogJava log) {
        byte[] bytes = gson.toJson(log).getBytes();
        System.out.println("Gson encoded #bytes: " +bytes.length);
        return bytes;
    }

//    public static byte[] serializeWithAvron(EventLogJava log) throws IOException {
//        Schema schema = ReflectData.get().getSchema(EventLogJava.class);
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        ReflectDatumWriter<EventLogJava> writer = new ReflectDatumWriter<>(EventLogJava.class);
//        DataFileWriter<EventLogJava> dataFileWriter = new DataFileWriter<>(writer);
//        dataFileWriter.create(schema,out);
//        dataFileWriter.append(log);
//        dataFileWriter.close();
//        out.close();
//        byte[] bytes = out.toByteArray();
//        System.out.println("Avro encoded #bytes: " +bytes.length);
//        return bytes;
//    }


    public static EventLogJava addArtificialActsUsingFiles(EventLogJava log) throws IOException {
        String importPath = File.createTempFile("process-mining-bridge-import", ".json").getAbsolutePath();
        String exportPath = File.createTempFile("process-mining-bridge-export", ".json").getAbsolutePath();
        Writer writer = new FileWriter(importPath);
        gson.toJson(log, writer);
        writer.flush();
        writer.close();
        String exportPathRes = addArtificialActsUsingFiles(importPath, exportPath);
        Reader reader = new FileReader(exportPath);
        deleteFile(importPath);
        EventLogJava res =  gson.fromJson(reader, EventLogJava.class);
        deleteFile(exportPath);
        return res;
    }

    public static void main(String[] args) throws IOException {
        int numTraces = 200_000;
        int numEventsPerTrace = 25;
        EventLogJava log = new EventLogJava(numTraces, numEventsPerTrace);

        int n = 5;
        System.out.println("--- byte[] JSON ---");
        double[] timingResults = new double[n];
        for (int i = 0; i < n; i++) {
            long startTime = System.nanoTime();
            EventLogJava res = addArtificialActs(log);
            timingResults[i] = (System.nanoTime() -
                    startTime) / 1000000.0;
        }
        System.out.println(Arrays.toString(timingResults));
        System.out.println("Average [ms]: " + Arrays.stream(timingResults).average().getAsDouble());
        System.out.println("-----------");
        System.out.println("--- File JSON ---");
        timingResults = new double[n];
        for (int i = 0; i < n; i++) {
            long startTime = System.nanoTime();
            EventLogJava res = addArtificialActsUsingFiles(log);
            timingResults[i] = (System.nanoTime() -
                    startTime) / 1000000.0;
        }
        System.out.println(Arrays.toString(timingResults));
        System.out.println("Average [ms]: " + Arrays.stream(timingResults).average().getAsDouble());
        System.out.println("-----------");
    }

}
