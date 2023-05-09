//===----------------------------------------------------------------------===//
//                         DuckDB
//
// inet-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class INETExtension : public Extension {
	static constexpr auto INET_TYPE_NAME = "inet";

public:
	static const LogicalType INETType() {
		child_list_t<LogicalType> children;
		children.push_back(make_pair("ip_type", LogicalType::UTINYINT));
		children.push_back(make_pair("address", LogicalType::HUGEINT));
		children.push_back(make_pair("mask", LogicalType::USMALLINT));
		LogicalType m_INETType = LogicalType::STRUCT(std::move(children));
		m_INETType.SetAlias(INET_TYPE_NAME);
		return m_INETType;
	}

public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
