package com.parallelmachines.reflex.common.events;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import com.parallelmachines.reflex.common.exceptions.ReflexCommonException;

import java.lang.reflect.Type;

/**
 * Class for handling generic events with JSON serialization/deserialization.
 * Events have the following JSON format:
 * {"type" : "ModelAccepted",
 * "info" : : {"modelId" : "id1234"}}
 *
 * 'info' can be an instance of any serializable class.
 * Or custom serializer/deserializer can be registered with Gson parser.
 * Classes must be mapped to EventType enum. (Check eventsBiMap further)
 *
 * eg, creation and serialization looks like this:
 * EventDescription ed = new EventDescription(new ModelAccepted("id1234"));
 * String js = ed.toJson();
 *
 * And deserialization:
 * EventDescription eventDescription = EventDescription.fromJson(js);
 * EventType eventType = eventDescription.getType();
 * switch (eventType)
 * {
 *      case ModelAccepted:
 *      someMethod(eventDescription);
 *      break;
 * }
 *
 * someMethod has fully defined type for EventDescription:
 * someMethod(EventDescription<ModelAccepted> eventDescription)
 *
 * In case of wrong type it will fail with casting Exception.
 */
public class EventDescription<T extends EventBaseClass> {
    @SerializedName("type")
    private EventType type;

    @SerializedName("info")
    private T info;

    public EventDescription(T event) {
        try {
            this.type = EventDescription.getEventTypeByClass(event.getClass());
        } catch (Exception e) {

        }
        this.info = event;
    }

    /**
     * Bidirectional map to handle EventType <-> Class
     */
    private static BiMap<EventType, Class<?>> eventsBiMap;

    static {
        eventsBiMap = HashBiMap.create();
        /**
         * It is not clear if Reflections package is well supported,
         * but it was referenced in many sources and is easy to use.
         * https://github.com/ronmamo/reflections
         *
         * If because of some reason it won't be available,
         * it is possible to implement classes introspection using java reflect APIs
         * or com.google.common.reflect.
         *
         * */

        /*
        add to pom
        <dependency>
			<groupId>org.reflections</groupId>
			<artifactId>reflections</artifactId>
			<version>0.9.11</version>
		</dependency>

        private static final String packagePrefix = "com.parallelmachines.reflex.common.events";

        Reflections reflections = new Reflections(packagePrefix);
        Set<Class<? extends EventBaseClass>> classes = reflections.getSubTypesOf(EventBaseClass.class);
        for (Class<? extends EventBaseClass> aClass : classes) {
            EventType et = null;
            try {
                // Note: event classes registration.
                // Every Event class has to override abstract getType() and return it's type.
                // Only non-static methods can be overriden, thus to call them we need to instantiate the class.
                // In order to to make things generic we call newInstance(),
                // which requires nullary constructor to be provided for every event class.

                et = aClass.newInstance().getType();
            } catch (InstantiationException | IllegalAccessException e) {
                String error = String.format("%s\nFor more information check documentation on the thrown exception (InstantiationException | IllegalAccessException)", e.toString());
                throw new ExceptionInInitializerError(error);
            }
            if (eventsBiMap.containsKey(et)) {
                String error = String.format("Trying to register class: %s.\nType %s has been already registered with class: %s", aClass.getCanonicalName(), et.toString(), eventsBiMap.get(et).getCanonicalName());
                throw new ExceptionInInitializerError(error);
            }
            eventsBiMap.put(et, aClass);
        }
        */
        eventsBiMap.put(EventType.ModelAccepted, ModelAccepted.class);
    }

    /**
     * Get class associated with EventType.
     *
     * Note: This method can be exposed if there will be a need to get Class for EventType,
     * to be able to create(or parse) object without using methods of current class.
     *
     * @param eventType EventType enum constant
     * @return event class
     */
    private static Class<?> getEventClassByType(EventType eventType) {
        if (!eventsBiMap.containsKey(eventType)) {
            throw new IllegalArgumentException(String.format("Event with type %s is not registered", eventType.toString()));
        }
        return eventsBiMap.get(eventType);
    }

    /**
     * Get event type associated with class.
     *
     * @param clazz Class of Event type
     * @return event class
     */
    private static EventType getEventTypeByClass(Class<?> clazz) throws ReflexCommonException {
        if (!eventsBiMap.inverse().containsKey(clazz)) {
            throw new ReflexCommonException(String.format("Event with class %s is not registered", clazz.getCanonicalName()));
        }
        return eventsBiMap.inverse().get(clazz);
    }

    /**
     * Get event body type in current event description.
     *
     * @return event type enum constant
     */
    public EventType getType() {
        return this.type;
    }

    /**
     * Get event body from current event description.
     *
     * @return event type enum constant
     */
    public T getEvent() {
        return this.info;
    }

    /**
     * Serialize current event description as a JSON.
     *
     * @return event type enum constant
     */
    public String toJson() {
        return new Gson().toJson(this);
    }

    /**
     * Class performs Event type resolution and deserialization.
     * Assuming event info consists only of simple types,
     * otherwise custom deserializers are needed. They can be implemented for every class and
     * registered during creation of eventsBiMap.
     */
    private static class EventDescDeserializer implements JsonDeserializer<EventDescription> {
        @Override
        public EventDescription deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
            JsonObject eventObject = json.getAsJsonObject();
            EventType eventType =  EventType.valueOf(eventObject.get("type").getAsString());
            JsonObject infoObject = eventObject.getAsJsonObject("info");

            Class<?> clazz = getEventClassByType(eventType);

            EventBaseClass eventObj = (EventBaseClass) new Gson().fromJson(infoObject, clazz);
            return new EventDescription(eventObj);
        }
    }

    private static GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter(EventDescription.class, new EventDescDeserializer());
    private static Gson gson = gsonBuilder.create();

    /**
     * Parse EventDescription from json
     *
     * @return event description or null
     * @throws ReflexCommonException
     */
    public static EventDescription fromJson(String js) throws ReflexCommonException {
        EventDescription ret = null;
        try {
            ret = gson.fromJson(js, EventDescription.class);
        } catch (Exception e) {
            String error = String.format("Can not parse EventDescription from: %s; %s", js, e.getMessage());
            throw (new ReflexCommonException(error));
        }
        return ret;
    }
}
