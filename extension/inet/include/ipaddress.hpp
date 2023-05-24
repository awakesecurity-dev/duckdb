//===----------------------------------------------------------------------===//
//                         DuckDB
//
// ipaddress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

enum class IPAddressType : uint8_t { IP_ADDRESS_INVALID = 0, IP_ADDRESS_V4 = 1, IP_ADDRESS_V6 = 2 };

namespace net {
	typedef union __address {
		uint32_t lvalue[4];
		uint16_t svalue[8];
		hugeint_t value;
	} ipaddress;
}

hugeint_t htonllll_le(hugeint_t hosthuge);
hugeint_t ntohllll_le(hugeint_t nethuge);

class IPAddress {
private:
	static bool TryParseIP4(string_t input, IPAddress &result, string *error_message);

public:
	constexpr static const int32_t IPV4_DEFAULT_MASK = 32;
	constexpr static const int32_t IPV6_DEFAULT_MASK = 128;

public:
	IPAddress();
	IPAddress(IPAddressType type, hugeint_t address, uint16_t mask);

	IPAddressType type;
	hugeint_t address;
	uint16_t mask;

public:
	static IPAddress FromIPv4(int32_t address, uint16_t mask);
	static IPAddress FromIPv6(hugeint_t address, uint16_t mask);
	static bool TryParse(string_t input, IPAddress &result, string *error_message);
	static IPAddress FromString(string_t input);

	string ToString() const;
};
} // namespace duckdb
