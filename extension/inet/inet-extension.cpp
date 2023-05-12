#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"
#include "inet-extension.hpp"
#include "inet_functions.hpp"

namespace duckdb {

void INETExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	auto inet_type = INETExtension::INETType();
	CreateTypeInfo info("inet", inet_type);
	info.temporary = true;
	info.internal = true;
	catalog.CreateType(*con.context, &info);

	// add inet functions
	auto host_fun = ScalarFunction("host", {inet_type}, LogicalType::VARCHAR, INetFunctions::Host);
	CreateScalarFunctionInfo host_info(host_fun);
	catalog.CreateFunction(*con.context, &host_info);

	auto subtract_fun = ScalarFunction("-", {inet_type, LogicalType::HUGEINT}, inet_type, INetFunctions::Subtract);
	CreateScalarFunctionInfo subtract_info(subtract_fun);
	subtract_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(*con.context, &subtract_info);

	auto isubtract_fun = ScalarFunction("-", {inet_type, inet_type}, LogicalType::HUGEINT, INetFunctions::InetSubtract);
	CreateScalarFunctionInfo isubtract_info(isubtract_fun);
	isubtract_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(*con.context, &isubtract_info);

	auto contain_within_fun = ScalarFunction(">>", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::ContainWithin);
	CreateScalarFunctionInfo contain_within_info(contain_within_fun);
	contain_within_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(*con.context, &contain_within_info);

	auto contain_within_eq_fun = ScalarFunction(">>=", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::ContainWithinOrEqual);
	CreateScalarFunctionInfo contain_within_eq_info(contain_within_eq_fun);
	contain_within_eq_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(*con.context, &contain_within_eq_info);

	auto contains_fun = ScalarFunction("<<", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::Contains);
	CreateScalarFunctionInfo contains_info(contains_fun);
	contains_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(*con.context, &contains_info);

	auto contains_eq_fun = ScalarFunction("<<=", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::ContainsOrEqual);
	CreateScalarFunctionInfo contains_eq_info(contains_eq_fun);
	contains_eq_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(*con.context, &contains_eq_info);

    auto add_fun = ScalarFunction("+", {inet_type, LogicalType::HUGEINT}, inet_type, INetFunctions::Add);
    CreateScalarFunctionInfo add_info(add_fun);
    add_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &add_info);

    auto sequal_fun = ScalarFunction("eq", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::Equal);
    CreateScalarFunctionInfo sequal_info(sequal_fun);
    sequal_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &sequal_info);

    auto snotequal_fun = ScalarFunction("neq", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::NotEqual);
    CreateScalarFunctionInfo snotequal_info(snotequal_fun);
    snotequal_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &snotequal_info);

    auto slessthan_fun = ScalarFunction("lt", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::LessThan);
    CreateScalarFunctionInfo slessthan_info(slessthan_fun);
    slessthan_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &slessthan_info);

    auto slsorequal_fun = ScalarFunction("lte", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::LessThanOrEqual);
    CreateScalarFunctionInfo slsorequal_info(slsorequal_fun);
    slsorequal_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &slsorequal_info);

    auto sgreaterthan_fun = ScalarFunction("gt", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::GreaterThan);
    CreateScalarFunctionInfo sgreaterthan_info(sgreaterthan_fun);
    sgreaterthan_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &sgreaterthan_info);

    auto sgtorequal_fun = ScalarFunction("gte", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::GreaterThanOrEqual);
    CreateScalarFunctionInfo sgtorequal_info(sgtorequal_fun);
    sgtorequal_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &sgtorequal_info);

    auto equal_fun = ScalarFunction("=", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::Equal);
    CreateScalarFunctionInfo equal_info(equal_fun);
    equal_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &equal_info);

    auto notequal_fun = ScalarFunction("<>", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::NotEqual);
    CreateScalarFunctionInfo notequal_info(notequal_fun);
    notequal_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &notequal_info);

    auto lessthan_fun = ScalarFunction("<", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::LessThan);
    CreateScalarFunctionInfo lessthan_info(lessthan_fun);
    lessthan_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &lessthan_info);

    auto lsorequal_fun = ScalarFunction("<=", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::LessThanOrEqual);
    CreateScalarFunctionInfo lsorequal_info(lsorequal_fun);
    lsorequal_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &lsorequal_info);

    auto greaterthan_fun = ScalarFunction(">", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::GreaterThan);
    CreateScalarFunctionInfo greaterthan_info(greaterthan_fun);
    greaterthan_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &greaterthan_info);

    auto gtorequal_fun = ScalarFunction(">=", {inet_type, inet_type}, LogicalType::BOOLEAN, INetFunctions::GreaterThanOrEqual);
    CreateScalarFunctionInfo gtorequal_info(gtorequal_fun);
    gtorequal_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &gtorequal_info);

	// add inet casts
	auto &config = DBConfig::GetConfig(*con.context);

	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(LogicalType::VARCHAR, inet_type, INetFunctions::CastVarcharToINET, 100);
	casts.RegisterCastFunction(inet_type, LogicalType::VARCHAR, INetFunctions::CastINETToVarchar);

	con.Commit();
}

std::string INETExtension::Name() {
	return "inet";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void inet_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::INETExtension>();
}

DUCKDB_EXTENSION_API const char *inet_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
