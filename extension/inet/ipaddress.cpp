#include "ipaddress.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

namespace duckdb {

hugeint_t htonllll_le(hugeint_t hosthuge) {
	net::ipaddress buf;

	buf.value = hosthuge;
	for (unsigned long i = 0; i < sizeof(buf.svalue) / sizeof(buf.svalue[0]); i += 2) {
		auto tmp = buf.svalue[(sizeof(buf.svalue) / sizeof(buf.svalue[0])) - 1 - i];
		buf.svalue[(sizeof(buf.svalue) / sizeof(buf.svalue[0])) - 1 - i] = htons(buf.svalue[i]);
		buf.svalue[i] = htons(tmp);
	}
	return buf.value;
}

hugeint_t ntohllll_le(hugeint_t nethuge) {
	net::ipaddress buf;

	buf.value = nethuge;
	for (unsigned long i = 0; i < sizeof(buf.svalue) / sizeof(buf.svalue[0]); i += 2) {
		auto tmp = buf.svalue[(sizeof(buf.svalue) / sizeof(buf.svalue[0])) - 1 - i];
		buf.svalue[(sizeof(buf.svalue) / sizeof(buf.svalue[0])) - 1 - i] = ntohs(buf.svalue[i]);
		buf.svalue[i] = ntohs(tmp);
	}
	return buf.value;
}

IPAddress::IPAddress() : type(IPAddressType::IP_ADDRESS_INVALID) {
}

IPAddress::IPAddress(IPAddressType type, hugeint_t address, uint16_t mask) : type(type), address(address), mask(mask) {
}

IPAddress IPAddress::FromIPv4(int32_t address, uint16_t mask) {
	return IPAddress(IPAddressType::IP_ADDRESS_V4, address, mask);
}
IPAddress IPAddress::FromIPv6(hugeint_t address, uint16_t mask) {
	return IPAddress(IPAddressType::IP_ADDRESS_V6, address, mask);
}

static bool IPAddressError(string_t input, string *error_message, string error) {
	string e = "Failed to convert string \"" + input.GetString() + "\" to inet: " + error;
	HandleCastError::AssignError(e, error_message);
	return false;
}

bool IPAddress::TryParse(string_t input, IPAddress &result, string *error_message) {
	std::string inbuf = std::string(input.GetDataUnsafe(), input.GetSize());
	const char* ipmask = (const char*)memchr(input.GetDataUnsafe(), '/', input.GetSize());
	std::string ipbuf = ipmask == NULL ? inbuf : std::string(input.GetDataUnsafe(), ipmask);
	net::ipaddress buf;
	int domain = AF_INET6;

	buf.value = 0;
	int s = inet_pton(domain, ipbuf.c_str(), &buf);
	if (s <= 0 && domain == AF_INET6) {
#ifdef USE_GLIBC_INET_PARSING
		// Note: libc implementation doesn't support ip address: 127.00.000.1
		s = inet_pton(domain = AF_INET, ipbuf.c_str(), &buf);
		if (s <= 0) return IPAddressError(input, error_message, "IPAddress not in presentation format");
		buf.lvalue[0] = ntohl(buf.lvalue[0]);
#else
		// Use duckdb implementation of IP4 parse which supports only little-endian architecture
		return IPAddress::TryParseIP4(input, result, error_message);
#endif
	} else {
		// valid only for little-endian architecture
		// should be fine here as hugeint_t and IPAddress assumes it runs only on little-endian architecture.
		//
		// Note: this implementation doesn't support ip address: a:b:c:d:e:f:192.168.000.1
		//       but do support: a.b.c.d.e.f.192.168.0.1
		buf.value = ntohllll_le(buf.value);
	}

	result.address = buf.value;
	result.type = domain == AF_INET ? IPAddressType::IP_ADDRESS_V4 : IPAddressType::IP_ADDRESS_V6;

	// parse mask
	unsigned long c = 0;
	if (ipmask == NULL) {
		// no mask, set to default
		result.mask = domain == AF_INET ? IPAddress::IPV4_DEFAULT_MASK : IPAddress::IPV6_DEFAULT_MASK;
		return true;
	}
	if (ipmask[c] != '/') {
		return IPAddressError(input, error_message, "Expected a slash");
	}
	c++;
	unsigned long start = c;
	while ((ipmask + c) < (input.GetDataUnsafe() + input.GetSize()) && ipmask[c] >= '0' && ipmask[c] <= '9') {
		c++;
	}
	uint8_t mask;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(ipmask + start, c - start), mask)) {
		return domain == AF_INET ? IPAddressError(input, error_message, "Expected a number between 0 and 32")
			: IPAddressError(input, error_message, "Expected a number between 0 and 128");
	}
	if (domain == AF_INET && mask > 32) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 32");
	} else if (mask > 128) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 128");
	}
	result.mask = mask;
	return true;
}

bool IPAddress::TryParseIP4(string_t input, IPAddress &result, string *error_message) {
	auto data = input.GetDataUnsafe();
	auto size = input.GetSize();
	idx_t c = 0;
	idx_t number_count = 0;
	uint32_t address = 0;
	result.type = IPAddressType::IP_ADDRESS_V4;
parse_number:
	idx_t start = c;
	while (c < size && data[c] >= '0' && data[c] <= '9') {
		c++;
	}
	if (start == c) {
		return IPAddressError(input, error_message, "Expected a number");
	}
	uint8_t number;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), number)) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 255");
	}
	address <<= 8;
	address += number;
	number_count++;
	result.address = address;
	if (number_count == 4) {
		goto parse_mask;
	} else {
		goto parse_dot;
	}
parse_dot:
	if (c == size || data[c] != '.') {
		return IPAddressError(input, error_message, "Expected a dot");
	}
	c++;
	goto parse_number;
parse_mask:
	if (c == size) {
		// no mask, set to default
		result.mask = IPAddress::IPV4_DEFAULT_MASK;
		return true;
	}
	if (data[c] != '/') {
		return IPAddressError(input, error_message, "Expected a slash");
	}
	c++;
	start = c;
	while (c < size && data[c] >= '0' && data[c] <= '9') {
		c++;
	}
	uint8_t mask;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), mask)) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 32");
	}
	if (mask > 32) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 32");
	}
	result.mask = mask;
	return true;
}

string IPAddress::ToString() const {
	char str[INET6_ADDRSTRLEN];
	net::ipaddress addr;

	addr.value = address;
	if (type == IPAddressType::IP_ADDRESS_V4) {
		addr.lvalue[0] = htonl(addr.lvalue[0]);
	} else {
		for (unsigned long i = 0; i < sizeof(addr.svalue) / sizeof(addr.svalue[0]); i += 2) {
			auto tmp = addr.svalue[(sizeof(addr.svalue) / sizeof(addr.svalue[0])) - 1 - i];
			addr.svalue[(sizeof(addr.svalue) / sizeof(addr.svalue[0])) - 1 - i] = htons(addr.svalue[i]);
			addr.svalue[i] = htons(tmp);
		}
	}
	int domain = type == IPAddressType::IP_ADDRESS_V4 ? AF_INET : AF_INET6;
	auto value = inet_ntop(domain, &addr, str, INET6_ADDRSTRLEN);
	std::string result = std::string(str);

	if (value == NULL) assert(0);
	if ((domain == AF_INET && mask != IPAddress::IPV4_DEFAULT_MASK) ||
	    (domain == AF_INET6 && mask != IPAddress::IPV6_DEFAULT_MASK)) {
		result += "/" + to_string(mask);
	}
	return result;
}

IPAddress IPAddress::FromString(string_t input) {
	IPAddress result;
	string error_message;
	if (!TryParse(input, result, &error_message)) {
		throw ConversionException(error_message);
	}
	return result;
}

} // namespace duckdb
