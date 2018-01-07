package com.amaken.airports

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

/** ********************************
  * Created by AliMaken on 2018/JAN/7
  * ********************************/

class AirportsSpec extends FunSuite with SharedSparkContext{

  // Not a real test. Just to force everything to run.
  test("It s'werks"){
    Airports.apply()
  }

}
