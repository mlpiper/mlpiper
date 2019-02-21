package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import breeze.linalg.DenseVector
import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexColumnEntry, ReflexNamedVector}
import org.apache.flink.streaming.scala.examples.common.parsing.ParameterIndices
import com.parallelmachines.reflex.common.enums.OpType

object HeatMapTestData {

  val onlineDataParameterIndices = new ParameterIndices(size = 3)

  val testDataStreamForHeatMap: Seq[DenseVector[Double]] = Seq[DenseVector[Double]](
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](30, 0.5, 50),
    DenseVector[Double](-10.0, 0.0, -100),
    DenseVector[Double](-20.0, -1.0, -50.0),
    DenseVector[Double](-20.0, -2.0, 50.0),
    DenseVector[Double](30.0, 3.0, -25.0),
    DenseVector[Double](-40.0, -9.0, 75.0),
    DenseVector[Double](60.0, 11.0, -20.0)
  )

  // NamedVectors
  val testDataStreamOfNamedVectorForHeatMap: Seq[ReflexNamedVector] = Seq[ReflexNamedVector](
    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 0.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 1.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "C", columnValue = 100.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 30.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 0.5, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "C", columnValue = 50.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = -10.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 0.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "C", columnValue = -100.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = -20.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = -1.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "C", columnValue = -50.0, OpType.CONTINUOUS))),


    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = -20.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = -2.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "C", columnValue = 50.0, OpType.CONTINUOUS))),


    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 30.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 3.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "C", columnValue = -25.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = -40.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = -9.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "C", columnValue = 75.0, OpType.CONTINUOUS))),

    ReflexNamedVector(Array[ReflexColumnEntry](ReflexColumnEntry(columnName = "A", columnValue = 60.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "B", columnValue = 11.0, OpType.CONTINUOUS),
      ReflexColumnEntry(columnName = "C", columnValue = -20.0, OpType.CONTINUOUS)))
  )

  // expected mean heatmap will be mean values of attributes for given minibatch and then min-max-scaling of average values
  val expectedMeanHeatMapForDoubleWindow_Mean_MinMax: Seq[Map[String, Double]] = Seq[Map[String, Double]](
    // average of 1, 2 entry -> avg1 = (15.0, 0.75, 75).
    // in global window only one entry -> avg1
    // min and max of global entries -> min = (15.0, 0.75, 75) & max = (15.0, 0.75, 75)
    // according to equation (x - min) / (max - min) -> First heatMap will be (0.0, 0.0, 0.0)
    Map[String, Double]("c0" -> 0.0, "c1" -> 0.0, "c2" -> 0.0),

    // 3,4  -> DenseVector[Double](-10.0, 0.0, -100), DenseVector[Double](-20.0, -1.0, -50.0),
    // avg of entry -> avg2 = (-15.0, -0.5, -75.0).
    // in global window only one entry -> avg1, avg2
    // min and max of global entries -> min = (-15.0, -0.5, -75.0) & max = (15.0, 0.75, 75)
    // according to equation Second heatMap will be (0.0, 0.0, 0.0)
    Map[String, Double]("c0" -> 0.0, "c1" -> 0.0, "c2" -> 0.0),

    // 5,6  -> DenseVector[Double](-20.0, -2.0, 50.0), DenseVector[Double](30.0, 3.0, -25.0)
    // avg of entry -> avg3 = (5.0, 0.5, 12.5).
    // in global window only one entry -> avg1, avg2, avg3
    // min and max of global entries -> min = (-15.0, -0.5, -75.0) & max = (15.0, 0.75, 75)
    // according to equation Third heatMap will be ((5 - -15 / 15 - -15), (0.5 - -0.5 / 0.75 - -0.5), (12.5 - -75 / 75 - -75)) => (0.66, 0.8, 0.58)
    Map[String, Double]("c0" -> 0.66, "c1" -> 0.8, "c2" -> 0.58),

    // 7,8  -> DenseVector[Double](-40.0, -9.0, 75.0), DenseVector[Double](60.0, 11.0, -20.0)
    // avg of entry -> avg4 = (10.0, 1.0, 27.5).
    // in global window only one entry -> avg2, avg3, avg4 (avg1 is dropped as max global window size is set to 3)
    // min and max of global entries -> min = (-15.0, -0.5, -75.0) & max = (10.0, 1.0, 27.5)
    // according to equation Third heatMap will be ((10 - -15 / 10 - -15), (1 - -0.5 / 1 - -0.5), (27.5 - -75 / 27.5 - -75)) => (1.0, 1.0, 1.0)
    Map[String, Double]("c0" -> 1.0, "c1" -> 1.0, "c2" -> 1.0)
  )

  // expected mean heatmap will be mean values of attributes for given minibatch and then min-max-scaling of average values
  val expectedMeanHeatMapForDoubleWindow_Mean_Standard: Seq[Map[String, Double]] = Seq[Map[String, Double]](
    // average of 1, 2 entry -> avg1 = (15.0, 0.75, 75).
    // in global window only one entry -> avg1
    // mean and std of global entries -> mean = (15.0, 0.75, 75) & std = (0, 0, 0)
    // according to equation x_scale = (x - mean) / (2*std) => (0.0, 0.0, 0.0)
    // and again scaling to (0, 1) equation x_heat = (x_scale + 1)/2
    // -> First heatMap will be (0.5, 0.5, 0.5)
    Map[String, Double]("c0" -> 0.5, "c1" -> 0.5, "c2" -> 0.5),

    // 3,4  -> DenseVector[Double](-10.0, 0.0, -100), DenseVector[Double](-20.0, -1.0, -50.0),
    // avg of entry -> avg2 = (-15.0, -0.5, -75.0).
    // in global window only one entry -> avg1, avg2
    // mean and std of global entries -> mean = (0.0, 0.125, 0) & std = (21.2132, 0.8838, 106.0660)
    // according to equation x_scale = ((-15 - 0.0) / (2*21.2132), (-0.5 - 0.125) / (2*0.8838), (-75.0 - 0) / (2*106.0660)) => (-0.3535, -0.3535, -0.3535)
    // and x_heat = ((-0.3535 + 1) / 2, (-0.3535 + 1) / 2, (-0.3535 + 1) / 2)) => (0.3232, 0.3232, 0.3232)
    Map[String, Double]("c0" -> 0.3232233, "c1" -> 0.3232233, "c2" -> 0.3232233),

    // 5,6  -> DenseVector[Double](-20.0, -2.0, 50.0), DenseVector[Double](30.0, 3.0, -25.0)
    // avg of entry -> avg3 = (5.0, 0.5, 12.5).
    // in global window only one entry -> avg1, avg2, avg3
    // mean and std of global entries -> mean = (1.6666, 0.25, 4.1666) & std = (15.2752, 0.6614, 75.34)
    // according to equation x_scale = ((5 - 1.6666) / (2*15.2752), (0.5 - 0.25) / (2*0.6614), (12.5 - 4.1666) / (2 *75.34)) => (0.1091, 0.1889, 0.5535)
    // and Third heatMap will be x_heat = ((0.1091 + 1) / 2, (0.1889 + 1) / 2, (0.5535 + 1) / 2)) => (0.5545, 0.5944, 0.5276)
    Map[String, Double]("c0" -> 0.55455447, "c1" -> 0.59449112, "c2" -> 0.52765006),

    // 7,8  -> DenseVector[Double](-40.0, -9.0, 75.0), DenseVector[Double](60.0, 11.0, -20.0)
    // avg of entry -> avg4 = (10.0, 1.0, 27.5).
    // in global window only one entry -> avg2, avg3, avg4 (avg1 is dropped as max global window size is set to 3)
    // mean and std of global entries -> mean = (0.0, 0.3333, -11.6667) & std = (13.2287, 0.7637, 55.3586)
    // according to equation x_scale = ((10 - 0) / (2*13.2287), (1 - 0.3333) / (2*0.7637), (27.5 - 11.6667) / (2*55.3586) => (0.3779, 0.4364, 0.3537)
    // and Fourth heatMap will be x_heat = ((0.7559 + 1) / 2, (0.8729 + 1) / 2, (0.7075 + 1) / 2)) => (0.6889, 0.7182, 0.6768)
    Map[String, Double]("c0" -> 0.68898224, "c1" -> 0.71821789, "c2" -> 0.67687681)
  )

  // expected mean heatmap will be mean values of attributes for given minibatch and then min-max-scaling of average values
  val expectedMeanHeatMapFor_NamedVector_DoubleWindow_Mean_Standard: Seq[Map[String, Double]] = Seq[Map[String, Double]](
    // average of 1, 2 entry -> avg1 = (15.0, 0.75, 75).
    // in global window only one entry -> avg1
    // mean and std of global entries -> mean = (15.0, 0.75, 75) & std = (0, 0, 0)
    // according to equation x_scale = (x - mean) / (2*std) => (0.0, 0.0, 0.0)
    // and again scaling to (0, 1) equation x_heat = (x_scale + 1)/2
    // -> First heatMap will be (0.5, 0.5, 0.5)
    Map[String, Double]("A" -> 0.5, "B" -> 0.5, "C" -> 0.5),

    // 3,4  -> DenseVector[Double](-10.0, 0.0, -100), DenseVector[Double](-20.0, -1.0, -50.0),
    // avg of entry -> avg2 = (-15.0, -0.5, -75.0).
    // in global window only one entry -> avg1, avg2
    // mean and std of global entries -> mean = (0.0, 0.125, 0) & std = (21.2132, 0.8838, 106.0660)
    // according to equation x_scale = ((-15 - 0.0) / (2*21.2132), (-0.5 - 0.125) / (2*0.8838), (-75.0 - 0) / (2*106.0660)) => (-0.3535, -0.3535, -0.3535)
    // and x_heat = ((-0.3535 + 1) / 2, (-0.3535 + 1) / 2, (-0.3535 + 1) / 2)) => (0.3232, 0.3232, 0.3232)
    Map[String, Double]("A" -> 0.3232233, "B" -> 0.3232233, "C" -> 0.3232233),

    // 5,6  -> DenseVector[Double](-20.0, -2.0, 50.0), DenseVector[Double](30.0, 3.0, -25.0)
    // avg of entry -> avg3 = (5.0, 0.5, 12.5).
    // in global window only one entry -> avg1, avg2, avg3
    // mean and std of global entries -> mean = (1.6666, 0.25, 4.1666) & std = (15.2752, 0.6614, 75.34)
    // according to equation x_scale = ((5 - 1.6666) / (2*15.2752), (0.5 - 0.25) / (2*0.6614), (12.5 - 4.1666) / (2 *75.34)) => (0.1091, 0.1889, 0.5535)
    // and Third heatMap will be x_heat = ((0.1091 + 1) / 2, (0.1889 + 1) / 2, (0.5535 + 1) / 2)) => (0.5545, 0.5944, 0.5276)
    Map[String, Double]("A" -> 0.55455447, "B" -> 0.59449112, "C" -> 0.52765006),

    // 7,8  -> DenseVector[Double](-40.0, -9.0, 75.0), DenseVector[Double](60.0, 11.0, -20.0)
    // avg of entry -> avg4 = (10.0, 1.0, 27.5).
    // in global window only one entry -> avg2, avg3, avg4 (avg1 is dropped as max global window size is set to 3)
    // mean and std of global entries -> mean = (0.0, 0.3333, -11.6667) & std = (13.2287, 0.7637, 55.3586)
    // according to equation x_scale = ((10 - 0) / (2*13.2287), (1 - 0.3333) / (2*0.7637), (27.5 - 11.6667) / (2*55.3586) => (0.3779, 0.4364, 0.3537)
    // and Fourth heatMap will be x_heat = ((0.7559 + 1) / 2, (0.8729 + 1) / 2, (0.7075 + 1) / 2)) => (0.6889, 0.7182, 0.6768)
    Map[String, Double]("A" -> 0.68898224, "B" -> 0.71821789, "C" -> 0.67687681)
  )

  // expected mean heatmap will be mean values of attributes for given minibatch after min-max-scaling of attributes
  val expectedMeanHeatMap: Seq[Map[String, Double]] = Seq[Map[String, Double]](
    Map[String, Double]("c0" -> 0.4, "c1" -> 0.5625, "c2" -> 0.5),
    Map[String, Double]("c0" -> 0.475, "c1" -> 0.4875, "c2" -> 0.45)
  )

  val testDataStreamOfConstantsForHeatMap: Seq[DenseVector[Double]] = Seq[DenseVector[Double]](
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](0.0, 1.0, 100)
  )

  // expected mean heatmap for constants should be 1 except for 0 constants
  val expectedMeanHeatMapOfConstants: Seq[Map[String, Double]] = Seq[Map[String, Double]](
    Map[String, Double]("c0" -> 0.0, "c1" -> 1.0, "c2" -> 1.0),
    Map[String, Double]("c0" -> 0.0, "c1" -> 1.0, "c2" -> 1.0))

  // expected mean heatmap for constants should be 1 except for 0 constants
  val expectedMeanHeatMapForWindowOfTwo: Seq[Map[String, Double]] = Seq[Map[String, Double]](
    Map[String, Double]("c0" -> 0.5, "c1" -> 0.5, "c2" -> 0.5),
    Map[String, Double]("c0" -> 0.5, "c1" -> 0.5, "c2" -> 0.5),
    Map[String, Double]("c0" -> 0.5, "c1" -> 0.5, "c2" -> 0.5),
    Map[String, Double]("c0" -> 0.5, "c1" -> 0.5, "c2" -> 0.5))

  val testDataStreamForHeatMapForHigherParallism: Seq[DenseVector[Double]] = Seq[DenseVector[Double]](
    DenseVector[Double](0.0, 1.0, 100),
    DenseVector[Double](-20.0, -2.0, 50.0),
    DenseVector[Double](30, 0.5, 50),
    DenseVector[Double](30.0, 3.0, -25.0),
    DenseVector[Double](-10.0, 0.0, -100),
    DenseVector[Double](-40.0, -9.0, 75.0),
    DenseVector[Double](-20.0, -1.0, -50.0),
    DenseVector[Double](60.0, 11.0, -20.0)
  )
}