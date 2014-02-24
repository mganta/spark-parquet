package com.cloudera.sa.simple

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

class SampleRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[HotelTaxes])
  }
}