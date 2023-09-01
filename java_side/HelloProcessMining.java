import java.util.ArrayList;
import java.util.HashMap;

class AttributeJava {
    private String name;
    private String value;

    public AttributeJava(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

class AttributesJava {
    private HashMap<String, String> attributes;

    public AttributesJava(ArrayList<AttributeJava> attributes) {
        this.attributes = new HashMap<>();
        for (AttributeJava attr : attributes) {
            this.attributes.put(attr.getName(), attr.getValue());
        }
    }

    public AttributesJava() {
        this.attributes = new HashMap<>();
    }

    public ArrayList<AttributeJava> getAttributes() {
        ArrayList<AttributeJava> ret = new ArrayList<>(
                this.attributes.entrySet().stream().map(e -> new AttributeJava(e.getKey(), e.getValue())).toList());
        return ret;
    }

    public String getAttribute(String name) {
        return this.attributes.get(name);
    }

    public void setAttribute(String name, String value) {
        this.attributes.put(name, value);
    }

}

class EventJava {
    private AttributesJava attributes;

    public EventJava(AttributesJava attributes) {
        this.attributes = attributes;
    }

    public AttributesJava getAttributes() {
        return attributes;
    }
}

class TraceJava {
    private AttributesJava attributes;
    private ArrayList<EventJava> events;

    public TraceJava(AttributesJava attributes, ArrayList<EventJava> events) {
        this.attributes = attributes;
        this.events = events;
    }

    public AttributesJava getAttributes() {
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
        int numTraces = 10_500;
        int numEventsPerTrace = 6;
        this.logName = "Test Log";
        this.traces = new ArrayList<>();
        for (int i = 0; i < numTraces; i++) {
            ArrayList<EventJava> events = new ArrayList<>();
            AttributesJava traceAttributeJavas = new AttributesJava();
            traceAttributeJavas.setAttribute(EventLogJava.CASE_ID_NAME, "Case " + i);
            TraceJava trace = new TraceJava(traceAttributeJavas, events);
            for (int j = 0; j < numEventsPerTrace; j++) {
                AttributesJava eventAttributeJavas = new AttributesJava();
                eventAttributeJavas.setAttribute(EventLogJava.ACTIVITY_NAME, "Activity " + j);
                events.add(new EventJava(eventAttributeJavas));
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

    private static native EventLogJava addArtificialActs(EventLogJava log);

    static {
        System.loadLibrary("java_bridge");
    }

    public static void main(String[] args) {
        EventLogJava log = new EventLogJava();

        System.out.println("Before Rust Call");
        long startTime = System.nanoTime();
        EventLogJava log2 = addArtificialActs(log);
        System.out.println("After Rust Call (took " + ((System.nanoTime() -
                startTime) / 1000000.0) + "ms)");


        System.out.println("Before Java Call");
        startTime = System.nanoTime();
        for (TraceJava trace : log.getTraces()) {
            AttributesJava startAttributes = new AttributesJava();
            startAttributes.setAttribute(EventLogJava.ACTIVITY_NAME, "__START__");
            EventJava startEvent = new EventJava(startAttributes);
            trace.getEvents().add(0,startEvent);
            AttributesJava endAttributes = new AttributesJava();
            endAttributes.setAttribute(EventLogJava.ACTIVITY_NAME, "__END__");
            EventJava endEvent = new EventJava(endAttributes);
            trace.getEvents().add(endEvent);

            // for (EventJava event : trace.getEvents()) {
            //     System.out.println(event.getAttributes().getAttribute(EventLogJava.ACTIVITY_NAME));
            // }
        }
        System.out.println("After Java Call (took " + ((System.nanoTime() -
                startTime) / 1000000.0) + "ms)");
    }

}
