package org.processmining.alpharevisitexperiments.bridge;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.model.*;
import org.deckfour.xes.model.impl.*;
import org.processmining.acceptingpetrinet.models.AcceptingPetriNet;
import org.processmining.acceptingpetrinet.models.impl.AcceptingPetriNetImpl;
import org.processmining.models.graphbased.NodeID;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.elements.Place;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.graphbased.directed.petrinet.impl.PetrinetFactory;
import org.processmining.models.semantics.petrinet.Marking;

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
    static Type xAttributeMapList = new TypeToken<XAttributeMapImpl[]>() {
    }.getType();
    static Type xAttributeList = new TypeToken<List<XAttribute>>() {
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

    private static native String addSampleDisconnectedNet(String petrinet);


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
        XAttributeMap[] traceAndEventAttrs = gson.fromJson(getCompleteRustTraceAsString(logPointer, traceIndex), xAttributeMapList);
        XAttributeMap traceAttrs = traceAndEventAttrs[0];
        XTraceImpl trace = IntStream.range(1, traceAndEventAttrs.length).boxed().map(i -> {
            XAttributeMap eventAttrs = traceAndEventAttrs[i];
            XID uuid = ((XAttributeIDImpl) eventAttrs.get("__UUID__")).getValue();
            eventAttrs.remove("__UUID__");
//            XAttributeMapImpl to = new XAttributeMapImpl(eventAttrs.size());
//            addAllAttributesFromTo(eventAttrs, to);
            return new XEventImpl(uuid, eventAttrs);
        }).collect(Collectors.toCollection(() -> new XTraceImpl(traceAttrs)));
        return trace;
    }


    private static XLog rustLogToJavaMultiEventChunks(long logPointer) {
        String logAttributesJson = getRustLogAttributes(logPointer);
        XAttributeMap logAttrsX = gson.fromJson(logAttributesJson, xAttributeMap);
        long numTraces = ((XAttributeDiscrete) logAttrsX.get("__NUM_TRACES__")).getValue();
        logAttrsX.remove("__NUM_TRACES__");
//        XAttributeMapImpl logAttrsX = convertToXAttributeMap(logAttrs);
        int chunks = Runtime.getRuntime().availableProcessors();
        ExecutorService execService = Executors.newFixedThreadPool(chunks);
        List<Future<XTrace>> futures = new ArrayList<>();
        for (int traceId = 0; traceId < numTraces; traceId++) {
            final int traceIndex = traceId;
            futures.add(execService.submit(() -> getxEvents(logPointer, traceIndex)));
        }
        return getxTraces(numTraces, logAttrsX, execService, futures);
    }

    private static XLog getxTraces(long numTraces, XAttributeMap logAttrsX, ExecutorService execService, List<Future<XTrace>> futures) {
        XLog newLog = futures.stream().map(f -> {
            try {
                return f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toCollection(() -> {
            XLogImpl tmp = new XLogImpl(logAttrsX);
            tmp.ensureCapacity((int) numTraces);
            return tmp;
        }));
        execService.shutdown();
        return newLog;
    }

    private static void setTraceParHelper(long pointer, Integer i, XTrace t, XAttributeMap traceAttributes) {
        XAttributeMap[] allTraceEventAttributes = t.stream().map(e -> e.getAttributes()).toArray(XAttributeMap[]::new);
        setTracePar(pointer, i, gson.toJson(traceAttributes), gson.toJson(allTraceEventAttributes, XAttributeMap[].class));
    }

    /**
     * Copies Java-side XLog to Rust <br/>
     * <b>Important</b>: Caller promises to eventually call destroyRustEventLog with returned pointer (long)
     *
     * @param l Java-side (XLog) Event Log to copy to Rust
     * @return Pointer to Rust-side event log (as long); Needs to be manually destroyed by caller!
     */
    private static long javaLogToRustMultiEventsChunked(XLog l) {
//        HashMap<String, String> attributes = new HashMap<>();
//        attributes.put("name", "Java-called Rust Log Par :)");
        long pointer = createRustEventLogPar(l.size(), gson.toJson(l.getAttributes()));
        System.out.println("Created Rust EventLog Pointer " + pointer);
        int chunks = Runtime.getRuntime().availableProcessors();
        ExecutorService execService = Executors.newFixedThreadPool(chunks);
        List<Future> futures = new ArrayList<>();
        for (int traceId = 0; traceId < l.size(); traceId++) {
            final int traceIndex = traceId;
            final XTrace t = l.get(traceIndex);
            futures.add(execService.submit(() -> {
//                HashMap<String, String> traceAttributes = new HashMap<>(t.getAttributes().size());
//                addAllAttributesFromTo(t.getAttributes(), traceAttributes);
                setTraceParHelper(pointer, traceIndex, t, t.getAttributes());
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
        long pointer = createRustEventLogPar(l.size(), gson.toJson(l.getAttributes()));

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

    public static XLog createRustEventLogHelperPar(XLog l) {
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

        return newLog;
    }

    private static String extractNodeID(NodeID nodeID) {
        return nodeID.toString().substring("node ".length());
    }

    public static AcceptingPetriNet bridgePetriNetFromWrapper(PetriNetBridge pnb) {
        Petrinet pn = PetrinetFactory.newPetrinet("Petri net");
        HashMap<String, Transition> transitionMap = new HashMap<>();
        HashMap<String, Place> placeMap = new HashMap<>();
        pnb.transitions.values().stream().forEach(t -> {
            Transition trans = pn.addTransition(t.label);
            if (t.label == null) {
                trans.setInvisible(true);
            }
            transitionMap.put(t.id, trans);
        });
        pnb.places.values().stream().forEach(p -> {
            placeMap.put(p.id, pn.addPlace(p.id));
        });
        pnb.arcs.forEach(arc -> {
            if (FromToBridge.PlaceTransition.equals(arc.from_to.type)) {
                pn.addArc(placeMap.get(arc.from_to.nodes[0]), transitionMap.get(arc.from_to.nodes[1]));
            } else {
                pn.addArc(transitionMap.get(arc.from_to.nodes[0]), placeMap.get(arc.from_to.nodes[1]));

            }
        });
        AcceptingPetriNet apn = new AcceptingPetriNetImpl(pn);
        if (pnb.initial_marking != null) {
            Marking im = new Marking();
            pnb.initial_marking.forEach((p, n) -> {
                for (int i = 0; i < n; i++) {
                    im.add(placeMap.get(p));
                }
            });
            apn.setInitialMarking(im);
        }
        if (pnb.final_markings != null) {
            apn.setFinalMarkings(new HashSet<>());
            Marking newFinalMarking = new Marking();
            for (HashMap<String, Integer> m : pnb.final_markings) {
                m.forEach((p, n) -> {
                    for (int i = 0; i < n; i++) {
                        newFinalMarking.add(placeMap.get(p));
                    }
                });
                apn.getFinalMarkings().add(newFinalMarking);
            }
        }
        return apn;
    }

    public static PetriNetBridge bridgePetriNetToWrapper(Petrinet net) {
        return bridgePetriNetToWrapper(net, null, null);
    }

    public static PetriNetBridge bridgePetriNetToWrapper(AcceptingPetriNet apn) {
        return bridgePetriNetToWrapper(apn.getNet(), apn.getInitialMarking(), apn.getFinalMarkings());
    }

    public static PetriNetBridge bridgePetriNetToWrapper(Petrinet net, Marking initialMarking, Set<Marking> finalMarkings) {
        PetriNetBridge pnb = new PetriNetBridge();

        for (org.processmining.models.graphbased.directed.petrinet.elements.Transition t : net.getTransitions()) {
            TransitionBridge tb = new TransitionBridge();
            tb.id = extractNodeID(t.getId());
            if (!t.isInvisible()) {
                tb.label = t.getLabel();
            }
            pnb.transitions.put(tb.id, tb);
        }

        for (org.processmining.models.graphbased.directed.petrinet.elements.Place t : net.getPlaces()) {
            PlaceBridge pb = new PlaceBridge();
            pb.id = extractNodeID(t.getId());
            pnb.places.put(pb.id, pb);
        }

        for (org.processmining.models.graphbased.directed.petrinet.PetrinetEdge<? extends org.processmining.models.graphbased.directed.petrinet.PetrinetNode, ? extends org.processmining.models.graphbased.directed.petrinet.PetrinetNode> e : net.getEdges()) {
            String sourceID = extractNodeID(e.getSource().getId());
            String targetID = extractNodeID(e.getTarget().getId());
            ArcBridge ab = new ArcBridge();
            ab.from_to.nodes = new String[]{sourceID, targetID};
            if (e.getSource() instanceof Place) {
                ab.from_to.type = FromToBridge.PlaceTransition;
            } else {
                ab.from_to.type = FromToBridge.TransitionPlace;
            }
            pnb.arcs.add(ab);
        }
        if (initialMarking != null) {
            for (Place p : initialMarking) {
                String placeID = extractNodeID(p.getId());
                pnb.initial_marking.put(placeID, pnb.initial_marking.getOrDefault(placeID, 0) + 1);
            }
        }
        if (finalMarkings != null) {
            for (Marking m : finalMarkings) {
                HashMap<String, Integer> finalMarking = new HashMap<>();
                for (Place p : m) {
                    String placeID = extractNodeID(p.getId());
                    finalMarking.put(placeID, finalMarking.getOrDefault(placeID, 0) + 1);
                }
                pnb.final_markings.add(finalMarking);
            }
        }
        return pnb;
    }

    public static AcceptingPetriNet javaPetriNetToRust(AcceptingPetriNet net) {
        String n = gson.toJson(bridgePetriNetToWrapper(net));
        String resJSON = addSampleDisconnectedNet(n);
        PetriNetBridge bridgeNet = gson.fromJson(resJSON, PetriNetBridge.class);
        return bridgePetriNetFromWrapper(bridgeNet);
    }

    public static XLog test(XLog l) {
        System.out.println("Start parallel Rust test on log with size " + l.size());
        return createRustEventLogHelperPar(l);
    }

    public static AcceptingPetriNet testNet(AcceptingPetriNet n) {
        return javaPetriNetToRust(n);
    }

    public static void main(String[] args) {
        String json = "{\n" + "  \"test\": {\n" + "    \"key\": \"test\",\n" + "    \"value\": {\n" + "      \"type\": \"String\",\n" + "      \"content\": \"Hello\"\n" + "    }\n" + "  },\n" + "  \"boolean-test\": {\n" + "    \"key\": \"boolean-test\",\n" + "    \"value\": {\n" + "      \"type\": \"Boolean\",\n" + "      \"content\": true\n" + "    }\n" + "  },\n" + "  \"date-test\": {\n" + "    \"key\": \"date-test\",\n" + "    \"value\": {\n" + "      \"type\": \"Date\",\n" + "      \"content\": 0\n" + "    }\n" + "  },\n" + "  \"container-test\": {\n" + "    \"key\": \"container-test\",\n" + "    \"value\": {\n" + "      \"type\": \"Container\",\n" + "      \"content\": {\n" + "        \"first\": {\n" + "          \"key\": \"first\",\n" + "          \"value\": {\n" + "            \"type\": \"Int\",\n" + "            \"content\": 1\n" + "          }\n" + "        },\n" + "        \"second\": {\n" + "          \"key\": \"second\",\n" + "          \"value\": {\n" + "            \"type\": \"Int\",\n" + "            \"content\": 2\n" + "          }\n" + "        },\n" + "        \"third\": {\n" + "          \"key\": \"third\",\n" + "          \"value\": {\n" + "            \"type\": \"Int\",\n" + "            \"content\": 3\n" + "          }\n" + "        }\n" + "      }\n" + "    }\n" + "  },\n" + "  \"float-test\": {\n" + "    \"key\": \"float-test\",\n" + "    \"value\": {\n" + "      \"type\": \"Float\",\n" + "      \"content\": 1.337\n" + "    }\n" + "  },\n" + "  \"list-test\": {\n" + "    \"key\": \"list-test\",\n" + "    \"value\": {\n" + "      \"type\": \"List\",\n" + "      \"content\": [\n" + "        {\n" + "          \"key\": \"first\",\n" + "          \"value\": {\n" + "            \"type\": \"Int\",\n" + "            \"content\": 1\n" + "          }\n" + "        },\n" + "        {\n" + "          \"key\": \"first\",\n" + "          \"value\": {\n" + "            \"type\": \"Float\",\n" + "            \"content\": 1.1\n" + "          }\n" + "        },\n" + "        {\n" + "          \"key\": \"second\",\n" + "          \"value\": {\n" + "            \"type\": \"Int\",\n" + "            \"content\": 2\n" + "          }\n" + "        }\n" + "      ]\n" + "    }\n" + "  },\n" + "  \"int-test\": {\n" + "    \"key\": \"int-test\",\n" + "    \"value\": {\n" + "      \"type\": \"Int\",\n" + "      \"content\": 42\n" + "    }\n" + "  },\n" + "  \"id-test\": {\n" + "    \"key\": \"id-test\",\n" + "    \"value\": {\n" + "      \"type\": \"ID\",\n" + "      \"content\": \"6f535ff1-5523-43f3-9e06-a61394c356ec\"\n" + "    }\n" + "  }\n" + "}";
//        XAttributeMap attributeMap = gson.fromJson(json, xAttributeMap);
//        System.out.println(attributeMap);
//        String backToJson = gson.toJson(attributeMap, xAttributeMap);
//        XAttributeMap backToMap = gson.fromJson(backToJson, xAttributeMap);
//        System.out.println(backToMap);
//        System.out.println(backToMap.equals(attributeMap));
//        System.out.println("Nice!");
        int numTraces = 20_000;
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

    private enum AttributeType {
        String("String"), Date("Date"), Int("Int"), Float("Float"), Boolean("Boolean"), ID("ID"), List("List"), Container("Container"), None("None");
        private String type;

        AttributeType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }

        public String getAttributeType() {
            return type;
        }
    }

    static class PlaceBridge {
        public String id;
    }

    static class TransitionBridge {
        public String id;
        public String label;

        public String getId() {
            return id;
        }

        public String getLabel() {
            return label;
        }
    }

    static class FromToBridge {
        static String PlaceTransition = "PlaceTransition";
        static String TransitionPlace = "TransitionPlace";
        public String type;
        public String[] nodes;
    }

    static class ArcBridge {
        public FromToBridge from_to = new FromToBridge();
        public int weight = 1;
    }

    static class PetriNetBridge {
        public HashMap<String, PlaceBridge> places = new HashMap<>();
        public HashMap<String, TransitionBridge> transitions = new HashMap<>();
        public ArrayList<ArcBridge> arcs = new ArrayList<>();
        public HashMap<String, Integer> initial_marking = new HashMap<>();
        public List<HashMap<String, Integer>> final_markings = new ArrayList<>();


        public HashMap<String, PlaceBridge> getPlaces() {
            return places;
        }

        public HashMap<String, TransitionBridge> getTransitions() {
            return transitions;
        }

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
            out.beginObject().name("key").value(value.getKey()).name("value").beginObject().name("type");
            if (value instanceof XAttributeLiteral) {
                out.value(AttributeType.String.type);
                out.name("content");
                out.value(((XAttributeLiteral) value).getValue());
            } else if (value instanceof XAttributeTimestamp) {
                out.value(AttributeType.Date.type);
                out.name("content");
                out.value(((XAttributeTimestamp) value).getValue().getTime());
            } else if (value instanceof XAttributeDiscrete) {
                out.value(AttributeType.Int.type);
                out.name("content");
                out.value(((XAttributeDiscrete) value).getValue());
            } else if (value instanceof XAttributeContinuous) {
                out.value(AttributeType.Float.type);
                out.name("content");
                out.value(((XAttributeContinuous) value).getValue());
            } else if (value instanceof XAttributeBoolean) {
                out.value(AttributeType.Boolean.type);
                out.name("content");
                out.value(((XAttributeBoolean) value).getValue());
            } else if (value instanceof XAttributeID) {
                out.value(AttributeType.ID.type);
                out.name("content");
                out.value(((XAttributeID) value).getValue().toString());
            } else if (value instanceof XAttributeList) {
                out.value(AttributeType.List.type);
                out.name("content");
                gson.toJson(((XAttributeList) value).getCollection(), xAttributeMapList, out);
            } else if (value instanceof XAttributeContainer) {
                out.value(AttributeType.Container.type);
                out.name("content");
                gson.toJson(((XAttributeContainer) value).getAttributes(), xAttributeMap, out);
            } else {
                throw new IOException("Unknown XAttribute type");
            }


            out.endObject().endObject();
        }


        @Override
        public XAttribute read(JsonReader in) throws IOException {
            in.beginObject();
            String keyName = in.nextName();
            assert keyName.equals("key");
            String key = in.nextString();
            String valueName = in.nextName();
            assert valueName.equals("value");
            in.beginObject();
            String typeName = in.nextName();
            assert typeName.equals("type");
            AttributeType type = AttributeType.valueOf(in.nextString());
            String contentName = in.nextName();
            assert contentName.equals("content");
            XAttribute attr = null;
            switch (type) {
                case String:
                    attr = new XAttributeLiteralImpl(key, in.nextString());
                    break;
                case Date:
                    attr = new XAttributeTimestampImpl(key, new Date(in.nextLong()));
                    break;
                case Int:
                    attr = new XAttributeDiscreteImpl(key, in.nextLong());
                    break;
                case Float:
                    attr = new XAttributeContinuousImpl(key, in.nextDouble());
                    break;
                case Boolean:
                    attr = new XAttributeBooleanImpl(key, in.nextBoolean());
                    break;
                case ID:
                    attr = new XAttributeIDImpl(key, XID.parse(in.nextString()));
                    break;
                case List:
                    XAttributeListImpl attrList = new XAttributeListImpl(key);
                    List<XAttribute> childAttrs = gson.fromJson(in, xAttributeList);
                    for (XAttribute c : childAttrs) {
                        attrList.addToCollection(c);
                    }
                    attr = attrList;
                    break;
                case Container:
                    XAttributeContainerImpl attrContainer = new XAttributeContainerImpl(key);
                    XAttributeMapImpl containedAttrs = gson.fromJson(in, xAttributeMap);
                    attrContainer.setAttributes(containedAttrs);
                    attr = attrContainer;
                    break;
            }
            in.endObject();
            in.endObject();
            return attr;
        }

    }
}
