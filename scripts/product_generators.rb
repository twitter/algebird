#!/usr/bin/env ruby

# @author Edwin Chen (@echen)
# Automatically write product semigroup, monoid, product group, and product ring
# classes for products up to size 22.
#
# Run it like this:
#
#   ruby scripts/product_generators.rb > algebird-core/src/main/scala/com/twitter/algebird/GeneratedProductAlgebra.scala
#
# TODO: Remove in favour of script/product_generators.rb in the next breaking release

PACKAGE_NAME = "com.twitter.algebird"

# The product sizes we want.
PRODUCT_SIZES = (2..22).to_a

# Each element in a product is of a certain type.
# This provides an alphabet to draw types from.
TYPE_SYMBOLS = ("A".."Z").to_a

INDENT = "  "

# This returns the comment for each product monoid/group/ring definition.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
#
# Example return:
#   "/**
#    * Combine two monoids into a product monoid
#    */"
def get_comment(n, algebraic_structure)
  ret = <<EOS
/**
 * Combine #{n} #{algebraic_structure}s into a product #{algebraic_structure}
 */
EOS
  ret.strip
end

# This returns the class definition for each product monoid/group/ring.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
#
# Example return:
#   "class Product2Monoid[T,U](implicit tmonoid: Monoid[T], umonoid: Monoid[U]) extends Monoid[(T,U)]"
def get_class_definition(n, algebraic_structure)
  # Example: "T,U"
  type_values_commaed = TYPE_SYMBOLS.first(n).join(", ")
  params = "(apply: (#{type_values_commaed}) => X, unapply: X => Option[(#{type_values_commaed})])"

  extends = ["#{algebraic_structure.capitalize}[X]"]
  parent = case algebraic_structure
  when "monoid"
    "Semigroup"
  when "group"
    "Monoid"
  when "ring"
    "Group"
  end

  if parent
    extends.unshift("Product#{n}#{parent}[X, #{type_values_commaed}]#{params}")
  end

  "class Product#{n}#{algebraic_structure.capitalize}[X, #{type_values_commaed}]#{params}(implicit #{get_type_parameters(n, algebraic_structure)}) extends #{extends.join(" with ")}"
end

# This returns the parameters for each product monoid/group/ring class.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
#
# Example return:
#   "tmonoid: Monoid[T], umonoid: Monoid[U]"
def get_type_parameters(n, algebraic_structure)
  params = TYPE_SYMBOLS.first(n).map{ |t| "#{t.downcase}#{algebraic_structure}: #{algebraic_structure.capitalize}[#{t.upcase}]"}
  params.join(", ")
end

# This returns the method definition for constants in the algebraic structure.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
# constant is "zero", "one", etc.
#
# Example return:
#   "override def zero = (tgroup.zero, ugroup.zero)"
def get_constant(n, algebraic_structure, constant)
  # Example: "tgroup.zero, ugroup.zero"
  constants_commaed = TYPE_SYMBOLS.first(n).map{ |t| "#{t.downcase}#{algebraic_structure}.#{constant}" }.join(", ")
  "override def #{constant} = apply(#{constants_commaed})"
end

# This returns the method definition for negation in the algebraic structure
# (assuming the structure has an additive inverse).
# n is the size of the product.
# algebraic_structure is "group", "ring", etc.
#
# Example return:
#   "override def negate(v: (T,U)) = (tgroup.negate(v._1), ugroup.negate(v._2))"
def get_negate(n, algebraic_structure)
  negates_commaed = TYPE_SYMBOLS.first(n).each_with_index.map{ |t, i| "#{t.downcase}#{algebraic_structure}.negate(tuple._#{i+1})" }.join(", ")
  "override def negate(v: X) = { val tuple = unapply(v).get; apply(#{negates_commaed}) }"
end

# This returns the method definition for associative binary operations in
# the algebraic structure.
# n is the size of the product.
# algebraic_structure is "monoid", "group", "ring", etc.
# operation is "plus", "minus", "times", etc.
#
# Example return:
#   "override def plus(l: (T,U), r: (T,U)) = (tmonoid.plus(l._1,r._1), umonoid.plus(l._2, r._2))"
def get_operation(n, algebraic_structure, operation)
  # Example: "(T, U)"
  individual_element_type = "X"

  # Example: "l: (T, U), r: (T, U)"
  method_params = "l: #{individual_element_type}, r: #{individual_element_type}" # (1..n).to_a.map{ |i| "x#{i}" }.map{ |p| "#{p}: #{individual_element_type}" }.join(", ")

  # Example: "(tmonoid.plus(l._1,r._1), umonoid.plus(l._2, r._2))"
  values_commaed = TYPE_SYMBOLS.first(n).each_with_index.map do |t, i|
    "#{t.downcase}#{algebraic_structure}.#{operation}(lTuple._#{i+1}, rTuple._#{i+1})"
  end.join(", ")
  values_commaed = "apply(#{values_commaed})"

  "override def #{operation}(#{method_params}) = { val lTuple = unapply(l).get; val rTuple = unapply(r).get; #{values_commaed} }"
end

def get_sumoption(n, bufferSize)
  # Example: "tsemigroup.sumOption(items.iterator.map(_._1), "
  values_commaed = TYPE_SYMBOLS.first(n).each_with_index.map do |t, i|
    "#{t.downcase}semigroup.sumOption(iter.map(_._#{i+1})).get"
  end.join(", ")

  "override def sumOption(to: TraversableOnce[X]) = {
    val buf = new ArrayBufferedOperation[X, X](#{bufferSize}) with BufferedReduce[X] {
      def operate(items: Seq[X]) = {
        val iter = items.iterator.map(unapply(_).get).toIterable
        apply(#{values_commaed})
      }
    }
    to.foreach(buf.put(_))
    buf.flush
  }"
end

# Example return:
#   "implicit def pairMonoid[T,U](implicit tg: Monoid[T], ug: Monoid[U]): Monoid[(T,U)] = {
#    new Product2Monoid[T,U]()(tg,ug)
#   }"
def get_implicit_definition(n, algebraic_structure)
  type_params_commaed = get_type_parameters(n, algebraic_structure)

  # Example: "T,U"
  product_type_commaed = TYPE_SYMBOLS.first(n).join(", ")

  # Example: "Monoid[(T,U)]"
  return_type = "#{algebraic_structure.capitalize}[(#{product_type_commaed})]"

  ret = %Q|#{INDENT}implicit def #{algebraic_structure}#{n}[#{product_type_commaed}](implicit #{type_params_commaed}): #{return_type} = {
#{INDENT}  new Product#{n}#{algebraic_structure.capitalize}[Tuple#{n}[#{product_type_commaed}], #{product_type_commaed}](Tuple#{n}.apply, Tuple#{n}.unapply)(#{TYPE_SYMBOLS.first(n).map{ |t| t.downcase + algebraic_structure.downcase }.join(", ")})
#{INDENT}}|
  ret
end

def get_product_definition(n, algebraic_structure)
  type_params_commaed = get_type_parameters(n, algebraic_structure)

  # Example: "T,U"
  product_type_commaed = TYPE_SYMBOLS.first(n).join(", ")

  ret = %Q|#{INDENT}def apply[X, #{product_type_commaed}](applyX: (#{ product_type_commaed}) => X, unapplyX: X => Option[(#{ product_type_commaed})])(implicit #{type_params_commaed}): #{algebraic_structure.capitalize}[X] = {
#{INDENT}  new Product#{n}#{algebraic_structure.capitalize}[X, #{product_type_commaed}](applyX, unapplyX)(#{TYPE_SYMBOLS.first(n).map{ |t| t.downcase + algebraic_structure.downcase }.join(", ")})
#{INDENT}}|
  ret
end


def print_class_definitions
  PRODUCT_SIZES.each do |product_size|

    code = <<EOS
#{get_comment(product_size, "semigroup")}
#{get_class_definition(product_size, "semigroup")} {
  #{get_operation(product_size, "semigroup", "plus")}
  #{get_sumoption(product_size, 1000)}
}

#{get_comment(product_size, "monoid")}
#{get_class_definition(product_size, "monoid")} {
  #{get_constant(product_size, "monoid", "zero")}
}

#{get_comment(product_size, "group")}
#{get_class_definition(product_size, "group")} {
  #{get_negate(product_size, "group")}
  #{get_operation(product_size, "group", "minus")}
}

#{get_comment(product_size, "ring")}
#{get_class_definition(product_size, "ring")} {
  #{get_constant(product_size, "ring", "one")}
  #{get_operation(product_size, "ring", "times")}
}
EOS

    puts code
  end
end

def print_custom_definitions
  puts "trait ProductSemigroups {"
  PRODUCT_SIZES.each do |n|
    puts get_product_definition(n, "semigroup")
    puts
  end
  puts "}"
  puts

  puts "trait ProductMonoids {"
  PRODUCT_SIZES.each do |n|
    puts get_product_definition(n, "monoid")
    puts
  end
  puts "}"
  puts

  puts "trait ProductGroups {"
  PRODUCT_SIZES.each do |n|
    puts get_product_definition(n, "group")
    puts
  end
  puts "}"
  puts

  puts "trait ProductRings {"
  PRODUCT_SIZES.each do |n|
    puts get_product_definition(n, "ring")
    puts
  end
  puts "}"
end

def print_implicit_definitions
  puts "trait GeneratedSemigroupImplicits {"
  PRODUCT_SIZES.each do |n|
    puts get_implicit_definition(n, "semigroup")
    puts
  end
  puts "}"
  puts

  puts "trait GeneratedMonoidImplicits {"
  PRODUCT_SIZES.each do |n|
    puts get_implicit_definition(n, "monoid")
    puts
  end
  puts "}"
  puts

  puts "trait GeneratedGroupImplicits {"
  PRODUCT_SIZES.each do |n|
    puts get_implicit_definition(n, "group")
    puts
  end
  puts "}"
  puts

  puts "trait GeneratedRingImplicits {"
  PRODUCT_SIZES.each do |n|
    puts get_implicit_definition(n, "ring")
    puts
  end
  puts "}"
end

puts "// following were autogenerated by #{__FILE__} at #{Time.now} do not edit"
puts "package #{PACKAGE_NAME}"
puts
print_class_definitions
puts
print_custom_definitions
# puts
# print_implicit_definitions
