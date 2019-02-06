package com.parallelmachines.reflex.common.events;

import com.parallelmachines.reflex.common.exceptions.ReflexCommonException;
import org.junit.Test;

import java.util.Arrays;

public class EventsTest {

    /**
     * EventDescription static registration works automatically by looking for all the classes
     * which inherit from EventBaseClass.
     * <p>
     * Thus it is hard to have negative path tests for some cases(registration failure due to mistake in Event class).
     * <p>
     * I've tested such cases and leave notes here:
     * - if nullary constructor is missing, exception will be thrown:
     * java.lang.ExceptionInInitializerError: java.lang.InstantiationException: com.parallelmachines.reflex.common.events.TestEventClass
     * For more information check documentation on the thrown exception (InstantiationException | IllegalAccessException)
     * <p>
     * - if two classes are trying to register for the same enum. exception will be thrown:
     * java.lang.ExceptionInInitializerError: Trying to register class: com.parallelmachines.reflex.common.events.ModelAccepted.
     * Type ModelAccepted has been already registered with class: com.parallelmachines.reflex.common.events.TestEventClass
     */

    @Test
    public void testSerializationDeserializtion() {
        String expectedJson = "{\"type\":\"ModelAccepted\", \"info\":{\"modelId\": \"id12345\"}}";
        char[] expectedChars = expectedJson.toCharArray();
        Arrays.sort(expectedChars);
        String expectedSorted = new String(expectedChars).replaceAll(" ", "");

        EventDescription originalEd = new EventDescription(new ModelAccepted("id12345"));
        String actualJson = originalEd.toJson();
        char[] actualChars = actualJson.toCharArray();
        Arrays.sort(actualChars);
        String actualSorted = new String(actualChars).replaceAll(" ", "");

        assert (expectedSorted.equals(actualSorted));

        EventDescription<ModelAccepted> restoredEd = null;
        try {
            restoredEd = EventDescription.fromJson(actualJson);
        } catch (ReflexCommonException e) {
            System.out.println(e.getMessage());
        }

        assert (restoredEd.getType() == originalEd.getType());
        assert (restoredEd.getEvent().getModelId().equals(((EventDescription<ModelAccepted>) originalEd).getEvent().getModelId()));
    }

    @Test(expected = ReflexCommonException.class)
    public void wrongMessage1() throws ReflexCommonException {
        String inputJson = "not a json";
        EventDescription.fromJson(inputJson);
    }

    @Test(expected = ReflexCommonException.class)
    public void wrongMessage2() throws ReflexCommonException {
        /* Wrong event type not existing in Enum,
         * or enum exists, but class was not registered.  */
        String inputJson = "{\"type\":\"WrongType\", \"info\":{\"modelId\": \"id12345\"}}";
        EventDescription.fromJson(inputJson);
    }

    @Test
    public void testEmptyOrNull() throws ReflexCommonException {
        assert (null == EventDescription.fromJson(null));
        assert (null == EventDescription.fromJson(""));
    }
}
