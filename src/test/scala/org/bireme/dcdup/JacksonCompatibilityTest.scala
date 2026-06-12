package org.bireme.dcdup

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JacksonCompatibilityTest extends AnyFlatSpec with Matchers {

  "The bundled NGrams shaded Jackson runtime" should
    "find the compatibility annotation classes it expects" in {
      noException should be thrownBy Class.forName("com.fasterxml.jackson.annotation.JsonSerializeAs")
      noException should be thrownBy Class.forName("com.fasterxml.jackson.annotation.JsonDeserializeAs")
    }

  it should "instantiate the shaded ObjectMapper without initializer errors" in {
    noException should be thrownBy new tools.jackson.databind.ObjectMapper()
  }
}
