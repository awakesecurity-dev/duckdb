//===----------------------------------------------------------------------===//
//                         DuckDB
//
// inet_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "ipaddress.hpp"


namespace duckdb {

using INET_TYPE = StructTypeTernary<uint8_t, hugeint_t, uint16_t>;

struct INetFunctions {
	static bool CastVarcharToINET(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static bool CastINETToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

	static void Host(DataChunk &args, ExpressionState &state, Vector &result);
	static void Subtract(DataChunk &args, ExpressionState &state, Vector &result);

	static void Contains(DataChunk &args, ExpressionState &state, Vector &result);
	static void ContainsOrEqual(DataChunk &args, ExpressionState &state, Vector &result);
	static void ContainWithin(DataChunk &args, ExpressionState &state, Vector &result);
	static void ContainWithinOrEqual(DataChunk &args, ExpressionState &state, Vector &result);

	static void InetSubtract(DataChunk &args, ExpressionState &state, Vector &result);
    static void Add(DataChunk &args, ExpressionState &state, Vector &result);
    static void Equal(DataChunk &args, ExpressionState &state, Vector &result);
    static void NotEqual(DataChunk &args, ExpressionState &state, Vector &result);
    static void LessThan(DataChunk &args, ExpressionState &state, Vector &result);
    static void LessThanOrEqual(DataChunk &args, ExpressionState &state, Vector &result);
    static void GreaterThan(DataChunk &args, ExpressionState &state, Vector &result);
    static void GreaterThanOrEqual(DataChunk &args, ExpressionState &state, Vector &result);

private:
    static bool InetContains(INET_TYPE source, INET_TYPE target);
    static bool InetContainsOrEqual(INET_TYPE source, INET_TYPE target);
    static bool InetEqual(const INET_TYPE source, const INET_TYPE target);
    static bool InetLessThan(const INET_TYPE source, const INET_TYPE target);
    static bool InetLessThanOrEqual(const INET_TYPE source, const INET_TYPE target);
};

} // namespace duckdb
