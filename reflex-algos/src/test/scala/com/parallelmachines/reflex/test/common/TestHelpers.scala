package com.parallelmachines.reflex.test.common

object TestHelpers {

  /**
    * Removes first occurrence of JSON pair which contains a token.
    * Function is designed barely as a test helper, parsing is based on matching commas and curly brackets.
    * This is developer's responsibility not to have these elements inside "quotes".
    *
    * Consider a json =
    * {
    *   "field1" : "value1",
    *   "field2" : value2,
    *   "field3" : "value3"
    * }
    *
    * call removeTokenPair(json, "value1") will return
    * {
    *   "field2" : value2,
    *   "field3" : "value3"
    * }
    *
    * call removeTokenPair(json, "field2") will return
    * {
    *   "field1" : "value1",
    *   "field3" : "value3"
    * }
    *
    * call removeTokenPair(json, "value3") will return
    * {
    *   "field1" : "value1",
    *   "field2" : value2
    * }
    *
    * @param s Input string
    * @param token A token in a json pair
    * @return Modified string
    */
  def removeTokenPair(s: String, token: String): String = {
    var r_index: Int = -1
    var l_index: Int = -1
    val token_idx = s.indexOf(token)
    if (token_idx < 0) {
      return s
    }
    var comma_r = s.indexOf(",", token_idx)
    var curly_r = s.indexOf("}", token_idx)
    r_index = if (comma_r > 0 && comma_r < curly_r) comma_r else curly_r

    var i = 0
    for (c <- s.substring(0, token_idx)) {
      if (c == '{' || c == ',') {
        l_index = i
      }
      i += 1
    }

    if (s.charAt(l_index) == '{') {
      l_index += 1
    }
    if (s.charAt(r_index) == ',' && s.charAt(l_index) != ',') {
      r_index += 1
    }
    return s.substring(0, l_index) + s.substring(r_index)
  }

}
