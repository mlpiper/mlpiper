package com.parallelmachines.reflex.test.reflexpipeline


import com.parallelmachines.reflex.factory.ComponentJSONSignatureParser
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory
import com.parallelmachines.reflex.common.constants.McenterTags


@RunWith(classOf[JUnitRunner])
class SignatureParserTest extends FlatSpec {

  private val logger = LoggerFactory.getLogger(getClass)


  DagTestUtil.initComponentFactory()

  "Good signature 1" should "be valid" in {

    val sig1 =
      """
        |{
        |  "engineType": "SparkPython",
        |  "language": "Python",
        |  "name": "mllib-random-forest",
        |  "label": "MlLib Random Forest",
        |  "program": "random_forest_classification_example.py",
        |  "modelBehavior": "ModelProducer",
        |  "inputInfo": [],
        |  "outputInfo": [],
        |  "group": "Algorithms",
        |  "arguments": [
        |    {
        |      "key": "num_trees",
        |      "label": "Number of trees",
        |      "type": "int",
        |      "description": "Number of trees",
        |      "optional": false
        |    },
        |    {
        |      "key": "num_classes",
        |      "label": "Number of classes",
        |      "type": "int",
        |      "description": "Number of classes",
        |      "optional": false
        |    },
        |    {
        |      "key": "output_dir",
        |      "label": "Model output dir",
        |      "type": "str",
        |      "description": "Directory for saving the trained model",
        |      "optional": false,
        |      "tag": "model_dir"
        |    },
        |    {
        |      "key": "data_file",
        |      "label": "Data file",
        |      "type": "str",
        |      "description": "Data file to use for input",
        |      "optional": false
        |    }
        |  ],
        |  "version": 1
        |}
        |
        """.stripMargin

    val parser = ComponentJSONSignatureParser
    val compMeta = parser.parseSignature(sig1)
    assert(compMeta.language.getOrElse(
      "bb") == "Python", "sig1 is not valid")
  }

  "Good signature 2" should "be valid" in {

    val sig2 =
      """
        |{
        |  "engineType": "Tensorflow",
        |  "name": "MnistDense",
        |  "label": "Mnist Dense",
        |  "program": "mnist_saved_model.py",
        |  "modelBehavior": "ModelProducer",
        |  "inputInfo": [],
        |  "outputInfo": [],
        |  "group": "Algorithms",
        |  "arguments": [
        |    {
        |      "key": "iterations",
        |      "label": "Number of Iterations",
        |      "type": "int",
        |      "description": "Number of training iterations",
        |      "optional": true,
        |      "defaultValue": 1000
        |    },
        |    {
        |      "key": "model_version",
        |      "label": "Model Version",
        |      "type": "str",
        |      "description": "Model version",
        |      "optional": true,
        |      "tag": "model_version"
        |    },
        |    {
        |      "key": "save_dir",
        |      "label": "Model output directory",
        |      "type": "str",
        |      "description": "Directory for saving the trained model",
        |      "optional": false,
        |      "tag": "model_dir"
        |    },
        |    {
        |      "key": "tf_log",
        |      "label": "Log directory",
        |      "type": "str",
        |      "description": "Tensorflow log directory",
        |      "optional": false,
        |      "tag": "tflog_dir"
        |    }
        |  ],
        |  "version": 1
        |}
        |
        """.stripMargin

    val parser = ComponentJSONSignatureParser
    val compMeta = parser.parseSignature(sig2)
    val model_version = compMeta.modelVersionArgument()
    assert(model_version == Some("model_version"))
    val model_dir = compMeta.modelDirArgument()
    assert(model_dir == Some("save_dir"))
  }

  "Good signature 3" should "be valid" in {

    val sig3 =
      """{
        |  "engineType": "Tensorflow",
        |  "name": "MnistCnnLayers",
        |  "label": "MnistCNNLayers",
        |  "program": "cnn_mnist.py",
        |  "modelBehavior": "ModelProducer",
        |  "inputInfo": [],
        |  "outputInfo": [],
        |  "group": "Algorithms",
        |  "arguments": [
        |    {
        |      "key": "step_size",
        |      "label": "Step Size",
        |      "type": "float",
        |      "description": "Learning rate",
        |      "optional": true,
        |      "defaultValue": 0.01
        |    },
        |    {
        |      "key": "iterations",
        |      "label": "Number of iterations",
        |      "type": "int",
        |      "description": "Number of training iterations",
        |      "optional": true,
        |      "defaultValue": 100
        |    },
        |    {
        |      "key": "batch_size",
        |      "label": "Batch size",
        |      "type": "int",
        |      "description": "Training batch input size",
        |      "optional": true,
        |      "defaultValue": 50
        |    },
        |    {
        |      "key": "model_version",
        |      "label": "Model version",
        |      "type": "str",
        |      "description": "Model version",
        |      "optional": true,
        |      "tag": "model_version"
        |    },
        |    {
        |      "key": "stats_interval",
        |      "label": "Statistics Interval",
        |      "type": "int",
        |      "description": "Print stats after this number of iterations",
        |      "optional": true,
        |      "defaultValue": 10
        |    },
        |    {
        |      "key": "save_dir",
        |      "label": "Model output dir",
        |      "type": "str",
        |      "description": "Directory for saving the trained model",
        |      "optional": true,
        |      "tag": "model_dir"
        |    },
        |    {
        |      "key": "input_dir",
        |      "label": "Input data directory",
        |      "type": "str",
        |      "description": "Directory for storing input data",
        |      "optional": true,
        |      "defaultValue": "/tmp/mnist_data"
        |    },
        |    {
        |      "key": "tf_log",
        |      "label": "Log directory",
        |      "type": "str",
        |      "description": "Tensorflow log directory",
        |      "optional": true,
        |      "defaultValue": "/tmp/tb_log",
        |      "tag": "tflog_dir"
        |    }
        |  ],
        |  "version": 1
        |}
      """.stripMargin
    val parser = ComponentJSONSignatureParser
    val compMeta = parser.parseSignature(sig3)
    val model_version = compMeta.modelVersionArgument()
    assert(model_version == Some("model_version"))
    val model_dir = compMeta.modelDirArgument()
    assert(model_dir == Some("save_dir"))
    val tb_log_dir = compMeta.logDirArgument()
    assert(tb_log_dir == Some("tf_log"))
  }

  "Good signature 4" should "be valid" in {

    val sig4 =
      """
        |{
        |  "engineType": "Python",
        |  "language": "Python",
        |  "name": "user-algorithm",
        |  "label": "user-algorithm",
        |  "program": "Dummy user algorithm",
        |  "modelBehavior": "ModelProducer",
        |  "inputInfo": [],
        |  "outputInfo": [],
        |  "group": "Algorithms",
        |  "arguments": [
        |    {
        |      "key": "num_trees",
        |      "label": "Number of trees",
        |      "type": "int",
        |      "description": "Number of trees",
        |      "optional": false
        |    },
        |    {
        |      "key": "num_classes",
        |      "label": "Number of classes",
        |      "type": "int",
        |      "description": "Number of classes",
        |      "optional": false
        |    },
        |    {
        |      "key": "output_dir",
        |      "label": "Model output dir",
        |      "type": "str",
        |      "description": "Directory for saving the trained model",
        |      "optional": false,
        |      "tag": "model_dir"
        |    },
        |    {
        |      "key": "data_file",
        |      "label": "Data file",
        |      "type": "str",
        |      "description": "Data file to use for input",
        |      "optional": false
        |    }
        |  ],
        |  "version": 1
        |}
        |
        """.stripMargin

    val parser = ComponentJSONSignatureParser
    val compMeta = parser.parseSignature(sig4)
    assert(compMeta.engineType == "Python", "sig4 engine is not correct")
    assert(compMeta.language.getOrElse("bb") == "Python", "sig4 is not valid")
  }


  "Bad signature 1" should "not be valid" in {

    // This signature has an extra , at the end --> optional: false,
    val sig1 =
      """
        |{
        |  "engineType": "SparkPython",
        |  "language": "Python",
        |  "name": "mllib-random-forest",
        |  "label": "MlLib Random Forest",
        |  "program": "random_forest_classification_example.py",
        |  "modelBehavior": "ModelProducer",
        |  "inputInfo": [],
        |  "outputInfo": [],
        |  "group": "Algorithms",
        |  "arguments": [
        |    {
        |      "key": "num_trees",
        |      "label": "Number of trees",
        |      "type": "int",
        |      "description": "Number of trees",
        |      "optional": false
        |    },
        |    {
        |      "key": "num_classes",
        |      "label": "Number of classes",
        |      "type": "int",
        |      "description": "Number of classes",
        |      "optional": false
        |    },
        |    {
        |      "key": "output_dir",
        |      "label": "Model output dir",
        |      "type": "str",
        |      "description": "Directory for saving the trained model",
        |      "optional": false,
        |      "tag": "model_dir"
        |    },
        |    {
        |      "key": "data_file",
        |      "label": "Data file",
        |      "type": "str",
        |      "description": "Data file to use for input",
        |      "optional": false,
        |    }
        |  ],
        |  "version": 1
        |}
        |
        """.stripMargin

    val parser = ComponentJSONSignatureParser
    intercept[Exception] {
      parser.parseSignature(sig1)
    }
  }


  "Good signature 5 - explainable" should "be valid" in {

    val sig3 =
      """{
        |  "engineType": "Tensorflow",
        |  "name": "MnistCnnLayers",
        |  "label": "MnistCNNLayers",
        |  "program": "cnn_mnist.py",
        |  "modelBehavior": "ModelProducer",
        |  "inputInfo": [],
        |  "outputInfo": [],
        |  "tags": ["explainable"],
        |  "group": "Algorithms",
        |  "arguments": [
        |    {
        |      "key": "step_size",
        |      "label": "Step Size",
        |      "type": "float",
        |      "description": "Learning rate",
        |      "optional": true,
        |      "defaultValue": 0.01
        |    },
        |    {
        |      "key": "iterations",
        |      "label": "Number of iterations",
        |      "type": "int",
        |      "description": "Number of training iterations",
        |      "optional": true,
        |      "defaultValue": 100
        |    },
        |    {
        |      "key": "batch_size",
        |      "label": "Batch size",
        |      "type": "int",
        |      "description": "Training batch input size",
        |      "optional": true,
        |      "defaultValue": 50
        |    },
        |    {
        |      "key": "model_version",
        |      "label": "Model version",
        |      "type": "str",
        |      "description": "Model version",
        |      "optional": true,
        |      "tag": "model_version"
        |    },
        |    {
        |      "key": "stats_interval",
        |      "label": "Statistics Interval",
        |      "type": "int",
        |      "description": "Print stats after this number of iterations",
        |      "optional": true,
        |      "defaultValue": 10
        |    },
        |    {
        |      "key": "save_dir",
        |      "label": "Model output dir",
        |      "type": "str",
        |      "description": "Directory for saving the trained model",
        |      "optional": true,
        |      "tag": "model_dir"
        |    },
        |    {
        |      "key": "input_dir",
        |      "label": "Input data directory",
        |      "type": "str",
        |      "description": "Directory for storing input data",
        |      "optional": true,
        |      "defaultValue": "/tmp/mnist_data"
        |    },
        |    {
        |      "key": "tf_log",
        |      "label": "Log directory",
        |      "type": "str",
        |      "description": "Tensorflow log directory",
        |      "optional": true,
        |      "defaultValue": "/tmp/tb_log",
        |      "tag": "tflog_dir"
        |    }
        |  ],
        |  "version": 1
        |}
      """.stripMargin
    val parser = ComponentJSONSignatureParser
    val compMeta = parser.parseSignature(sig3)
    val model_version = compMeta.modelVersionArgument()
    assert(model_version == Some("model_version"))
    val model_dir = compMeta.modelDirArgument()
    assert(model_dir == Some("save_dir"))
    val tb_log_dir = compMeta.logDirArgument()
    assert(tb_log_dir == Some("tf_log"))
    val explainability_tags = compMeta.getTags()
    assert(explainability_tags.get.contains(McenterTags.explainable))
  }
}
