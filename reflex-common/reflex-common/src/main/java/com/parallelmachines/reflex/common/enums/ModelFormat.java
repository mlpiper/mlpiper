package com.parallelmachines.reflex.common.enums;

import com.google.gson.annotations.SerializedName;

import java.util.EnumSet;
import java.util.Set;

/**
 * Format of the model depends on the engine and package used to create the model
 */
public enum ModelFormat {
	@SerializedName("Pmml")
	PMML ("Pmml"),
	@SerializedName("Json")
	JSON ("Json"),
	@SerializedName("Savedmodel")
	SAVEDMODEL ("Savedmodel"), // TensorFlow model
	@SerializedName("SparkML")
	SPARKML ("SparkML"),
	@SerializedName("Binary")
	BINARY ("Binary"),
	@SerializedName("Text")
	TEXT ("Text"),

	// Scikitlearn models generated in python3 are not compatible with python2
	// Thus we use 2 different types
	@SerializedName("ScikitLearn_2")
	SCIKITLEARN_2 ("ScikitLearn_2"),

	@SerializedName("ScikitLearn_3")
	SCIKITLEARN_3 ("ScikitLearn_3"),

	// H2O Opensource project
	@SerializedName("H2O_3")
	H2O_3 ("H2O_3"),

	// H2O Driverless model
	@SerializedName("H2O_Driveless_Ai")
	H2O_DRIVERLESS_AI ("H2O_Driverless{Ai"),

	@SerializedName("Unknown")
	UNKNOWN ("Unknown");

	// TODO: set the is directory or is file flag in DB
	// Now, SPARKPMML and SAVEDMODEL are marked as directory. JSON, BINARY, PMML, TEXT and UNKNOWN are marked as file.
	public static final Set<ModelFormat> DIRECTORY = EnumSet.of(SPARKML, SAVEDMODEL);
	public static final Set<ModelFormat> FILE = EnumSet.of(JSON, BINARY, TEXT, PMML, UNKNOWN, SCIKITLEARN_2,
			SCIKITLEARN_3, H2O_3, H2O_DRIVERLESS_AI);

	// TODO: add the link set here

	private final String name;

	ModelFormat(String name) {
		this.name = name;
	}

	public static ModelFormat fromString(String name) {
		if (name != null) {
			for (ModelFormat mf : ModelFormat.values()) {
				if (name.equalsIgnoreCase(mf.name)) {
					return mf;
				}
			}
		}
		return UNKNOWN;
	}

	@Override
	public String toString() {
		return name;
	}

	public static boolean isPackagedAsFile(ModelFormat modelFormat) {
		return FILE.contains(modelFormat);
	}

	public static boolean isPackagedAsDirectory(ModelFormat modelFormat) {
		return DIRECTORY.contains(modelFormat);
	}

	public static boolean needToExtractModel(ModelFormat modelFormat) {
		return modelFormat == SPARKML || modelFormat == SAVEDMODEL;
	}
}
