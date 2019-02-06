package com.parallelmachines.reflex.common.events;

import com.google.gson.annotations.SerializedName;

/**
 * Class to handle model accepted event.
 */
public class ModelAccepted extends EventBaseClass{
    @SerializedName("modelId")
    private String modelId;

    @Override
    public EventType getType() {
        return EventType.ModelAccepted;
    }

    /**
     * ModelAccepted constructor
     * @param modelId id of the accepted model
     */
    public ModelAccepted(String modelId) {
        this.modelId = modelId;
    }

    /**
     * ModelAccepted constructor
     *
     * nullary constructor is required for registration.
     */
    public ModelAccepted() {}

    /**
     * Get model id
     * @return model id
     */
    public String getModelId() {
        return this.modelId;
    }
}
