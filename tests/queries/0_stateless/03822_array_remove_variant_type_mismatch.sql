-- Test for issue #95839: arrayRemove with incompatible Variant types should not crash

-- This query involves complex nested arrays with Variant types where the array elements
-- and the element to remove have incompatible types. Previously this would cause a crash
-- with "Logical error: '(isConst() || isSparse() || isReplicated()) ? getDataType() == rhs.getDataType() : typeid(*this) == typeid(rhs)'"
-- due to equals() returning Nullable(Nothing) for incompatible Variant comparisons.

SELECT arrayRemove(
    [
        [[isNotDistinctFrom(16, isNotDistinctFrom(16, assumeNotNull(isNotNull(materialize(8)))))]],
        [[materialize(toUInt128(8)), equals(2, isNull(isZeroOrNull(*)))], *],
        [isNotDistinctFrom(isNull(assumeNotNull(16)), isNotDistinctFrom(isZeroOrNull(NULL), 16)), [], [arrayMap(x -> materialize(0), [NULL])]],
        [isZeroOrNull(8), [isZeroOrNull(8)]]
    ],
    [[arrayRemove(['hello', 'world'], concat('a', 1, equals(16, isNullable(8)))), isNotDistinctFrom(16, isNotDistinctFrom(isZeroOrNull(8), 16))]]
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Simpler test case: arrayRemove with incompatible Variant element types
SELECT arrayRemove([[1, 2], [3, 4]], [['hello']]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
