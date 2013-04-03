package com.twitter.algebird

class HashingTrickMonoid[V:Group](bits : Int, seed : Int = 123456) extends Monoid[AdaptiveVector[V]] {
	val vectorSize = 1 << bits
	val bitMask = vectorSize - 1
	val hash = MurmurHash128(seed)

	val zero = AdaptiveVector.fill[V](vectorSize)(Monoid.zero[V])

	def plus(left : AdaptiveVector[V], right : AdaptiveVector[V]) = Monoid.plus(left, right)

	def init(key : Array[Byte], value : V) = {
		val (long1, long2) = hash(key)
		val index = (long1 & bitMask).toInt
		val isNegative = (long2 & 1) == 1

		val signedValue = if(isNegative) Group.negate(value) else value
		AdaptiveVector.fromMap[V](Map(index -> signedValue), Monoid.zero[V], vectorSize)
	}
}