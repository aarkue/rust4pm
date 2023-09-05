package org.processmining.alpharevisitexperiments.bridge;

import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.model.*;
import org.deckfour.xes.model.impl.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("UnstableApiUsage")
public class HelloProcessMining {
    private static final Set<String> relevantKeys = new HashSet<>(Arrays.asList("concept:name", "case:concept:name", "name"));
    static Gson gson = new GsonBuilder().disableHtmlEscaping().registerTypeAdapter(XAttribute.class, new XAttributeTypeAdapter()).registerTypeAdapter(XEvent.class, new XEventTypeAdapter()).registerTypeAdapter(XEventImpl.class, new XEventTypeAdapter()).registerTypeAdapter(XTrace.class, new XTraceTypeAdapter()).registerTypeAdapter(XTraceImpl.class, new XTraceTypeAdapter()).registerTypeAdapter(XLog.class, new XLogTypeAdapter()).registerTypeAdapter(XLogImpl.class, new XLogTypeAdapter()).create();
    static Type xTraceListType = new TypeToken<List<XTrace>>() {
    }.getType();
    static Type xEventListType = new TypeToken<List<XEvent>>() {
    }.getType();
    static Type xAttributeMap = new TypeToken<XAttributeMapImpl>() {
    }.getType();
    static Type stringHashMap = new TypeToken<HashMap<String, String>>() {
    }.getType();

    static {
        System.load("/home/aarkue/doc/projects/rust-bridge-process-mining/target/release/libjava_bridge.so");
//        System.loadLibrary("java_bridge");

    }


    private static native long createRustEventLogPar(int numTraces, String logAttributes);
    private static native void setTracePar(long constructionPointer, int traceIndex, String traceAttributes, String eventAttributes);
    private static native void setTraceParJsonCompatible(long constructionPointer, int traceIndex, String traceJSON);
    private static native long finishLogConstructionPar(long constructionPointer);
    private static native boolean destroyRustEventLog(long pointer);
    private static native void addStartEndToRustLog(long eventLogPointer);
    private static native String getRustLogAttributes(long eventLogPointer);

    private static native int[] getRustTraceLengths(long eventLogPointer);
    private static native String getCompleteRustTraceAsString(long eventLogPointer, int index);

    private static native String getCompleteRustTraceAsStringJsonCompatible(long eventLogPointer, int index);

    private static native String getCompleteRustLogAsStringJsonCompatible(long eventLogPointer);






    private static void addAllAttributesFromTo(XAttributeMap from, Map<String, String> to) {
        addAllAttributesFromTo(from, to, false);
    }

    private static void addAllAttributesFromTo(XAttributeMap from, Map<String, String> to, boolean onlyRelevant) {
        if (onlyRelevant) {
            Sets.intersection(from.keySet(), relevantKeys).forEach(key -> to.put(key, from.get(key).toString()));
        } else {
            from.forEach((key, value) -> to.put(key, value.toString()));
        }
    }

    private static void addAllAttributesFromTo(Map<String, String> from, XAttributeMap to) {
        from.forEach((key, value) -> to.put(key, new XAttributeLiteralImpl(key, value)));
    }

    private static XAttributeMapImpl convertToXAttributeMap(Map<String, String> from) {
        XAttributeMapImpl to = new XAttributeMapImpl(from.size());
        addAllAttributesFromTo(from, to);
        return to;
    }

    private static XTraceImpl getxEvents(long logPointer, Integer traceIndex) {
        Map<String, String>[] traceAndEventAttrs = gson.fromJson(getCompleteRustTraceAsString(logPointer, traceIndex), Map[].class);
        XAttributeMapImpl traceAttrs = convertToXAttributeMap(traceAndEventAttrs[0]);
        XTraceImpl trace = IntStream.range(1, traceAndEventAttrs.length).boxed().map(i -> {
            Map<String, String> eventAttrs = traceAndEventAttrs[i];
            String uuid = eventAttrs.get("__UUID__");
            eventAttrs.remove("__UUID__");
            XAttributeMapImpl to = new XAttributeMapImpl(eventAttrs.size());
            addAllAttributesFromTo(eventAttrs, to);
            return new XEventImpl(XID.parse(uuid), to);
        }).collect(Collectors.toCollection(() -> new XTraceImpl(traceAttrs)));
        return trace;
    }


    private static XLog rustLogToJavaMultiEventChunks(long logPointer) {
        String logAttributesJson = getRustLogAttributes(logPointer);
        HashMap<String, String> logAttrs = gson.fromJson(logAttributesJson, stringHashMap);
        int numTraces = Integer.parseInt(logAttrs.get("__NUM_TRACES__"));
        logAttrs.remove("__NUM_TRACES__");
        XAttributeMapImpl logAttrsX = convertToXAttributeMap(logAttrs);
        int chunks = Runtime.getRuntime().availableProcessors();
        ExecutorService execService = Executors.newFixedThreadPool(chunks);
        List<Future<XTrace>> futures = new ArrayList<>();
        for (int traceId = 0; traceId < numTraces; traceId++) {
            final int traceIndex = traceId;
            futures.add(execService.submit(() -> getxEvents(logPointer, traceIndex)));
        }
        return getxTraces(numTraces, logAttrsX, execService, futures);
    }

    private static XLog getxTraces(int numTraces, XAttributeMapImpl logAttrsX, ExecutorService execService, List<Future<XTrace>> futures) {
        XLog newLog = futures.stream().map(f -> {
            try {
                return f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toCollection(() -> {
            XLogImpl tmp = new XLogImpl(logAttrsX);
            tmp.ensureCapacity(numTraces);
            return tmp;
        }));
        execService.shutdown();
        return newLog;
    }

    private static void setTraceParHelper(long pointer, Integer i, XTrace t, HashMap<String, String> traceAttributes) {
        HashMap[] allTraceEventAttributes = t.stream().map(e -> {
            HashMap<String, String> eventAttributes = new HashMap<>(e.getAttributes().size());
            addAllAttributesFromTo(e.getAttributes(), eventAttributes);
            return eventAttributes;
        }).toArray(HashMap[]::new);
        setTracePar(pointer, i, gson.toJson(traceAttributes), gson.toJson(allTraceEventAttributes, HashMap[].class));
    }

    /**
     * Copies Java-side XLog to Rust <br/>
     * <b>Important</b>: Caller promises to eventually call destroyRustEventLog with returned pointer (long)
     *
     * @param l Java-side (XLog) Event Log to copy to Rust
     * @return Pointer to Rust-side event log (as long); Needs to be manually destroyed by caller!
     */
    private static long javaLogToRustMultiEventsChunked(XLog l) {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("name", "Java-called Rust Log Par :)");
        long pointer = createRustEventLogPar(l.size(), gson.toJson(attributes));

        int chunks = Runtime.getRuntime().availableProcessors();
        ExecutorService execService = Executors.newFixedThreadPool(chunks);
        List<Future> futures = new ArrayList<>();
        for (int traceId = 0; traceId < l.size(); traceId++) {
            final int traceIndex = traceId;
            final XTrace t = l.get(traceIndex);
            futures.add(execService.submit(() -> {
                HashMap<String, String> traceAttributes = new HashMap<>(t.getAttributes().size());
                addAllAttributesFromTo(t.getAttributes(), traceAttributes);
                setTraceParHelper(pointer, traceIndex, t, traceAttributes);
            }));
        }
        for (Future f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        execService.shutdown();
        return finishLogConstructionPar(pointer);
    }

    private static long javaLogToRustMultiEventsJsonCompatibleChunked(XLog l) {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("name", "Java-called Rust Log Par :)");
        long pointer = createRustEventLogPar(l.size(), gson.toJson(attributes));

        int chunks = Runtime.getRuntime().availableProcessors();
        ExecutorService execService = Executors.newFixedThreadPool(chunks);
        List<Future> futures = new ArrayList<>();
        for (int traceId = 0; traceId < l.size(); traceId++) {
            final int traceIndex = traceId;
            final XTrace t = l.get(traceIndex);
            futures.add(execService.submit(() -> {
                String tJSON = gson.toJson(t, XTraceImpl.class);
                setTraceParJsonCompatible(pointer, traceIndex, tJSON);
            }));
        }
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        execService.shutdown();
        return finishLogConstructionPar(pointer);
    }

    public static double createRustEventLogHelperPar(XLog l) {
        System.out.println("createRustEventLogHelperPar #Traces:" + l.size());
        long startTime = System.nanoTime();
        long logPointer = javaLogToRustMultiEventsChunked(l);
        System.out.println("[Java] Finish Log & Added all traces; Pointer: " + logPointer);
        System.out.println("[Java] Took " + ((System.nanoTime() - startTime) / 1000000.0) + "ms");

        long addStartEndStart = System.nanoTime();
        addStartEndToRustLog(logPointer);

        double addStartEndDuration = ((System.nanoTime() - addStartEndStart) / 1000000.0);
        System.out.println("[Java] Finished adding start/end acts; Took " + addStartEndDuration + "ms");

        long backToJavaStart = System.nanoTime();
        XLog newLog = rustLogToJavaMultiEventChunks(logPointer);

        double backToJavaDuration = ((System.nanoTime() - backToJavaStart) / 1000000.0);
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

        double duration = ((System.nanoTime() - startTime) / 1000000.0);
        System.out.println("Call took " + duration + "ms");

        return duration;
    }

    public static void test(XLog l) {
        createRustEventLogHelperPar(l);
        System.out.println("Finished parallel Rust test on log with size " + l.size());
    }

    public static void main(String[] args) {
        int numTraces = 200_000;
        int numEventsPerTrace = 20;
        XLog xlog = createHugeXLog(numTraces, numEventsPerTrace);
        createRustEventLogHelperPar(xlog);
    }

    private static XLog createHugeXLog(int numTraces, int numEventsPerTrace) {
        XAttributeMapImpl logAttrs = new XAttributeMapImpl();
        logAttrs.put("name", new XAttributeLiteralImpl("name", "Huge Test Log"));
        XLogImpl log = new XLogImpl(logAttrs);
        for (int i = 0; i < numTraces; i++) {
            XAttributeMapImpl traceAttrs = new XAttributeMapImpl();
            traceAttrs.put("case:concept:name", new XAttributeLiteralImpl("case:concept:name", "Trace " + i));
            XTraceImpl trace = new XTraceImpl(traceAttrs);
            for (int j = 0; j < numEventsPerTrace; j++) {
                XAttributeMapImpl eventAttrs = new XAttributeMapImpl();
                eventAttrs.put("concept:name", new XAttributeLiteralImpl("concept:name", "Activity " + j));
                XEventImpl ev = new XEventImpl(eventAttrs);
                trace.add(ev);
            }
            log.add(trace);
        }
        return log;
    }

    private static class XEventTypeAdapter extends TypeAdapter<XEvent> {
        @Override
        public void write(JsonWriter out, XEvent value) throws IOException {
            out.beginObject().name("uuid").value(value.getID().toString());
            out.name("attributes");
            gson.toJson(value.getAttributes(), xAttributeMap, out);
            out.endObject();
        }


        @Override
        public XEvent read(JsonReader in) throws IOException {
            in.beginObject();
            in.nextName(); // uuid
            String uuid = in.nextString();
            in.nextName(); // attributes
            XAttributeMap map = gson.fromJson(in, xAttributeMap);
            in.endObject();
            return new XEventImpl(XID.parse(uuid), map);
        }
    }

    private static class XTraceTypeAdapter extends TypeAdapter<XTrace> {
        @Override
        public void write(JsonWriter out, XTrace value) throws IOException {
            out.beginObject().name("attributes");
            gson.toJson(value.getAttributes(), xAttributeMap, out);
            out.name("events");
            gson.toJson(value, List.class, out);
            out.endObject();
        }


        @Override
        public XTrace read(JsonReader in) throws IOException {
            in.beginObject();
            in.nextName(); // attributes
            XAttributeMap map = gson.fromJson(in, xAttributeMap);
            in.nextName();
            List<XEvent> events = gson.fromJson(in, xEventListType);
            in.endObject();
            XTraceImpl trace = new XTraceImpl(map);
            trace.addAll(events);
            return trace;
        }
    }

    private static class XLogTypeAdapter extends TypeAdapter<XLog> {
        @Override
        public void write(JsonWriter out, XLog value) throws IOException {
            out.beginObject().name("attributes");
            gson.toJson(value.getAttributes(), xAttributeMap, out);
            out.name("traces");
            gson.toJson(value, List.class, out);
            out.endObject();
        }


        @Override
        public XLog read(JsonReader in) throws IOException {
            in.beginObject();
            in.nextName(); // attributes
            XAttributeMap map = gson.fromJson(in, xAttributeMap);
            in.nextName();
            List<XTrace> traces = gson.fromJson(in, xTraceListType);
            in.endObject();
            XLogImpl log = new XLogImpl(map);
            log.addAll(traces);
            return log;
        }
    }


    private static class XAttributeTypeAdapter extends TypeAdapter<XAttribute> {
        @Override
        public void write(JsonWriter out, XAttribute value) throws IOException {
            out.beginObject().name("key").value(value.getKey()).name("attributeType").value("string").name("value").value(value.toString()).endObject();
        }

        @Override
        public XAttribute read(JsonReader in) throws IOException {
            in.beginObject();
            String keyName = in.nextName();
//            assert keyName.equals("key");
            String key = in.nextString();
            String typeName = in.nextName();
//            assert typeName.equals("attributeType");
            String attributeType = in.nextString();
            String valueName = in.nextName();
//            assert valueName.equals("value");
            String value = in.nextString();
            in.endObject();
            return new XAttributeLiteralImpl(key, value);
        }
    }
}
