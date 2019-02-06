package com.parallelmachines.reflex.common.events;

import com.parallelmachines.reflex.common.ReflexEventJava.ReflexEvent.EventType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EventHelper {
	public static List<EventType> getAlertTypes() {
		return Arrays.asList(
							EventType.Anomaly,
							EventType.Alert,
							EventType.CanaryHealth,
							EventType.GenericDataAlert,
							EventType.GenericHealthAlert,
							EventType.GenericSystemAlert);
	}

	public static List<EventType> getAllEventTypes() {
		List<EventType> all = new ArrayList<EventType>(EventHelper.getAlertTypes());
		all.add(EventType.GenericEvent);
		all.add(EventType.ModelAccepted);
		return all;
	}

	public static boolean isGenericAlert(EventType type) {
		switch (type) {
			case GenericDataAlert:
			case GenericHealthAlert:
			case GenericSystemAlert:
				return true;
			default:
				return false;
		}
	}
}

