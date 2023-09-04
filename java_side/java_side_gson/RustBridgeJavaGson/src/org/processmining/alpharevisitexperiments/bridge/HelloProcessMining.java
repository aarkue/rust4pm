package org.processmining.alpharevisitexperiments.bridge;

import com.google.gson.Gson;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class EventJava {
    private HashMap<String, String> attributes;

    public EventJava() {

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

    public void setAttributes(HashMap<String, String> attributes) {
        this.attributes = attributes;
    }
}

class TraceJava {
    private HashMap<String, String> attributes;
    private List<EventJava> events;

    public TraceJava() {

    }

    public TraceJava(String caseID) {
        this(caseID, new ArrayList<>());
    }

    public TraceJava(String caseID, List<EventJava> events) {
        this.attributes = new HashMap<>();
        this.attributes.put(EventLogJava.CASE_ID_NAME, caseID);
        this.events = events;
    }

    public TraceJava(HashMap<String, String> attributes, List<EventJava> events) {
        this.attributes = attributes;
        this.events = events;
    }

    public void addEvent(EventJava event) {
        this.events.add(event);
    }

    public HashMap<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(HashMap<String, String> attributes) {
        this.attributes = attributes;
    }

    public List<EventJava> getEvents() {
        return events;
    }

    public void setEvents(List<EventJava> events) {
        this.events = events;
    }
}

class EventLogJava {
    public static final String CASE_ID_NAME = "case:concept:name";
    public static final String ACTIVITY_NAME = "concept:name";

    private HashMap<String, String> attributes;
    private List<TraceJava> traces;

    public EventLogJava() {

    }

    public EventLogJava(String logName, List<TraceJava> traces) {
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

    public List<TraceJava> getTraces() {
        return this.traces;
    }

    public void setTraces(List<TraceJava> traces) {
        this.traces = traces;
    }

    public HashMap<String, String> getAttributes() {
        return this.attributes;
    }

    public void setAttributes(HashMap<String, String> attributes) {
        this.attributes = attributes;
    }
}

public class HelloProcessMining {
    static Gson gson = new Gson();

    static {
        System.load("/home/aarkue/doc/projects/rust-bridge-process-mining/target/release/libjava_bridge.so");
//        System.loadLibrary("java_bridge");
    }

    public static void deleteFile(String filePath) {
        File file = new File(filePath);
        file.delete();
    }

    private static native byte[] addArtificialActs(byte[] data);

    private static native byte[] addArtificialActsAvro(byte[] data);

    private static native long createRustEventLog(String attributes);

    private static native boolean destroyRustEventLog(long pointer);

    private static native long appendTrace(long eventLogPointer, String attributes);

    private static native long appendEventToLastTrace(long eventLogPointer, String attributes);

    private static native long createRustEventLogPar(int numTraces, String logAttributes);

    private static native long setTraceAttributesPar(long constructionPointer, int index, String traceAttributes);

    private static native long addEventToTracePar(long constructionPointer, int index, String eventAttributes);

    private static native long finishLogConstructionPar(long constructionPointer);

    private static native void addStartEndToRustLog(long eventLogPointer);

    private static native int[] getRustTraceLengths(long eventLogPointer);

    private static native String getRustLogAttributes(long eventLogPointer);


    private static native String getRustTraceAttributes(long eventLogPointer, int traceIndex);

    private static native String getRustEventAttributes(long eventLogPointer, int traceIndex, int eventIndex);


    private static native String addArtificialActsUsingFiles(String importPath, String exportPath);

    public static EventLogJava addArtificialActs(EventLogJava log) {
        byte[] bytes = gson.toJson(log).getBytes();
        System.out.println("#Bytes: " + bytes.length);
        bytes = addArtificialActs(bytes);
        return gson.fromJson(new String(bytes), EventLogJava.class);
    }

    public static byte[] serializeWithGson(EventLogJava log) {
        byte[] bytes = gson.toJson(log).getBytes();
        System.out.println("Gson encoded #bytes: " + bytes.length);
        return bytes;
    }

    public static void serializeWithGson(Object log) throws IOException {

//        String json = gson.toJson(log);
        File exportPath = new File("xlog-test.json");
        Writer writer = new FileWriter(exportPath);
        gson.toJson(log, writer);
//        System.out.println("Gson encoded #bytes: " + json.length());
//        return json;
    }


    private static void addAllAttributesFromTo(XAttributeMap from, HashMap<String, String> to) {
        from.entrySet().stream().forEach(entry -> {
            to.put(entry.getKey(), entry.getValue().toString());
        });
    }

    private static void addAllAttributesFromTo(HashMap<String, String> from, XAttributeMap to) {
        from.entrySet().stream().forEach(entry -> {
            to.put(entry.getKey(), new XAttributeLiteralImpl(entry.getKey(), entry.getValue().toString()));
        });
    }

    public static EventLogJava convertToWrapper(XLog l) {
        EventLogJava log = new EventLogJava();
        log.setAttributes(new HashMap<>());
        addAllAttributesFromTo(l.getAttributes(), log.getAttributes());
        log.setTraces(l.stream().map(t -> {
            TraceJava trace = new TraceJava();
            trace.setEvents(new ArrayList<>());
            trace.setAttributes(new HashMap<>());
            addAllAttributesFromTo(t.getAttributes(), trace.getAttributes());
            for (XEvent e : t) {
                EventJava event = new EventJava();
                event.setAttributes(new HashMap<>());
                addAllAttributesFromTo(e.getAttributes(), event.getAttributes());
                trace.addEvent(event);
            }
            return trace;
        }).collect(Collectors.toList()));
        return log;
    }

    private static void iterateXLogStreamTest(XLog l) {
        long startTime = System.nanoTime();
        l.parallelStream().forEach(t -> {
            HashMap<String, String> traceAttributes = new HashMap<>();
            addAllAttributesFromTo(t.getAttributes(), traceAttributes);
            for (XEvent e : t) {
                HashMap<String, String> eventAttributes = new HashMap<>();
                addAllAttributesFromTo(e.getAttributes(), eventAttributes);

            }
        });
        System.out.println("Iterating Log (Stream) took " + ((System.nanoTime() -
                startTime) / 1000000.0) + "ms");
    }

    private static void iterateXLogTest(XLog l) {
        long startTime = System.nanoTime();
        for (XTrace t : l) {
            HashMap<String, String> traceAttributes = new HashMap<>();
            addAllAttributesFromTo(t.getAttributes(), traceAttributes);
            for (XEvent e : t) {
                HashMap<String, String> eventAttributes = new HashMap<>();
                addAllAttributesFromTo(e.getAttributes(), eventAttributes);

            }
        }
        System.out.println("Iterating Log took " + ((System.nanoTime() -
                startTime) / 1000000.0) + "ms");
    }

    private static XAttributeMapImpl convertToXAttributeMap(HashMap<String, String> from) {
        XAttributeMapImpl to = new XAttributeMapImpl();
        addAllAttributesFromTo(from, to);
        return to;
    }

    private static XLog rustLogToJava(long logPointer) {
        int[] traceLengths = getRustTraceLengths(logPointer);
        String logAttributesJson = getRustLogAttributes(logPointer);
        HashMap<String, String> logAttrs = gson.fromJson(logAttributesJson, HashMap.class);
        XLogImpl newLog = new XLogImpl(convertToXAttributeMap(logAttrs));
        List<XTraceImpl> traces = IntStream.range(0, traceLengths.length).boxed().parallel().map(traceIndex -> {
            String traceAttrsJson = getRustTraceAttributes(logPointer, traceIndex);
            HashMap<String, String> traceAttrs = gson.fromJson(traceAttrsJson, HashMap.class);
            XTraceImpl trace = new XTraceImpl(convertToXAttributeMap(traceAttrs));
            List<XEventImpl> x = IntStream.range(0, traceLengths[traceIndex]).boxed().map(eventIndex -> {
                String eventAttrsJson = getRustEventAttributes(logPointer, traceIndex, eventIndex);
                HashMap<String, String> eventAttrs = gson.fromJson(eventAttrsJson, HashMap.class);
                XEventImpl event = new XEventImpl(convertToXAttributeMap(eventAttrs));
                return event;
            }).collect(Collectors.toList());
            trace.addAll(x);
            return trace;
        }).collect(Collectors.toList());
        newLog.addAll(traces);
        return newLog;
    }

    //
    /**
     * Copies Java-side XLog to Rust <br/>
     * <b>Important</b>: Caller promises to eventually call destroyRustEventLog with returned pointer (long)
     * @param l Java-side (XLog) Event Log to copy to Rust
     * @return Pointer to Rust-side event log (as long); Needs to be manually destroyed by caller!
     */
    private static long javaLogToRust(XLog l){
        HashMap<String, String> attributes = new HashMap<String, String>();
        attributes.put("name", "Java-called Rust Log Par :)");
        long pointer = createRustEventLogPar(l.size(), gson.toJson(attributes));
        IntStream.range(0, l.size()).boxed().parallel().forEach(i -> {
            XTrace t = l.get(i);
            HashMap<String, String> traceAttributes = new HashMap<>(t.getAttributes().size());
            addAllAttributesFromTo(t.getAttributes(), traceAttributes);
            setTraceAttributesPar(pointer, i, gson.toJson(traceAttributes));
            for (XEvent e : t) {
                HashMap<String, String> eventAttributes = new HashMap<>(e.getAttributes().size());
                addAllAttributesFromTo(e.getAttributes(), eventAttributes);
                addEventToTracePar(pointer, i, gson.toJson(eventAttributes));

            }
        });
        long logPointer = finishLogConstructionPar(pointer);
        return logPointer;
    }

    public static double createRustEventLogHelperPar(XLog l) {
        System.out.println("createRustEventLogHelperPar");
        long startTime = System.nanoTime();
        long logPointer = javaLogToRust(l);
        System.out.println("[Java] Finish Log & Added all traces; Pointer: " + logPointer);
        System.out.println("[Java] Took " + ((System.nanoTime() -
                startTime) / 1000000.0) + "ms");
        long addStartEndStart = System.nanoTime();
        addStartEndToRustLog(logPointer);
        double addStartEndDuration = ((System.nanoTime() -
                addStartEndStart) / 1000000.0);
        System.out.println("[Java] Finished adding start/end acts; Took " + addStartEndDuration + "ms");

        long backToJavaStart = System.nanoTime();
//      Now get back the EventLog to Java Land!
        XLog newLog = rustLogToJava(logPointer);
        double backToJavaDuration = ((System.nanoTime() -
                backToJavaStart) / 1000000.0);
        System.out.println("Back to Java took " + backToJavaDuration + "ms");

        System.out.println("Got XLog of size: " + newLog.size());
        System.out.println("First trace is:");
        for (XEvent e : newLog.get(0)) {
            System.out.println(e.getAttributes().get("concept:name"));
        }
        System.out.println("---");


//        Important!
        boolean d = destroyRustEventLog(logPointer);
//        ^ Important to destroy RustEventLog when no longer needed; Else memory is leaked.
        System.out.println("[Java] After destroy " + d);
        double duration = ((System.nanoTime() -
                startTime) / 1000000.0);
        System.out.println("Call took " + duration + "ms");
        return duration;
    }

    public static void test(XLog l) {
        createRustEventLogHelperPar(l);
        System.out.println("Finished parallel Rust test on log with size " + l.size());
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
        EventLogJava res = gson.fromJson(reader, EventLogJava.class);
        deleteFile(exportPath);
        return res;
    }

    public static void main(String[] args) throws IOException {
        int numTraces = 200_000;
        int numEventsPerTrace = 10;
        EventLogJava log = new EventLogJava(numTraces, numEventsPerTrace);

        int n = 1;
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
//        System.out.println("--- File JSON ---");
//        timingResults = new double[n];
//        for (int i = 0; i < n; i++) {
//            long startTime = System.nanoTime();
//            EventLogJava res = addArtificialActsUsingFiles(log);
//            timingResults[i] = (System.nanoTime() -
//                    startTime) / 1000000.0;
//        }
//        System.out.println(Arrays.toString(timingResults));
//        System.out.println("Average [ms]: " + Arrays.stream(timingResults).average().getAsDouble());
//        System.out.println("-----------");
    }

}
