#include "inet_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

#include <assert.h>

namespace duckdb {

bool INetFunctions::CastVarcharToINET(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto &entries = StructVector::GetEntries(result);
	auto ip_type = FlatVector::GetData<uint8_t>(*entries[0]);
	auto address_data = FlatVector::GetData<hugeint_t>(*entries[1]);
	auto mask_data = FlatVector::GetData<uint16_t>(*entries[2]);

	auto input = (string_t *)vdata.data;
	bool success = true;
	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		IPAddress inet;
		if (!IPAddress::TryParse(input[idx], inet, parameters.error_message)) {
			FlatVector::SetNull(result, i, true);
			success = false;
			continue;
		}
		ip_type[i] = uint8_t(inet.type);
		address_data[i] = inet.address;
		mask_data[i] = inet.mask;
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return success;
}

bool INetFunctions::CastINETToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	GenericExecutor::ExecuteUnary<INET_TYPE, PrimitiveType<string_t>>(source, result, count, [&](INET_TYPE input) {
		IPAddress inet(IPAddressType(input.a_val), input.b_val, input.c_val);
		auto str = inet.ToString();
		return StringVector::AddString(result, str);
	});
	return true;
}

void INetFunctions::Host(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteUnary<INET_TYPE, PrimitiveType<string_t>>(
	    args.data[0], result, args.size(), [&](INET_TYPE input) {
		    IPAddress inet(IPAddressType(input.a_val),
                    input.b_val,
                    IPAddressType(input.a_val) == IPAddressType::IP_ADDRESS_V4 ? IPAddress::IPV4_DEFAULT_MASK : IPAddress::IPV6_DEFAULT_MASK);
		    auto str = inet.ToString();
		    return StringVector::AddString(result, str);
	    });
}

void INetFunctions::Subtract(DataChunk &args, ExpressionState &state, Vector &result) {
    GenericExecutor::ExecuteBinary<INET_TYPE, PrimitiveType<hugeint_t>, INET_TYPE>(
        args.data[0], args.data[1], result, args.size(), [&](INET_TYPE ip, PrimitiveType<hugeint_t> val) {
            if (IPAddressType(ip.a_val) == IPAddressType::IP_ADDRESS_V4) {
                if (val.val > UINT32_MAX) throw NotImplementedException("Out of range!?");
            }
            auto new_address = ip.b_val - val.val;

            if ( ((IPAddressType(ip.a_val) == IPAddressType::IP_ADDRESS_V4) && (new_address < 0 || new_address > UINT32_MAX)) ||
                 ((IPAddressType(ip.a_val) == IPAddressType::IP_ADDRESS_V6) &&
                  ((new_address > 0 && ip.b_val < 0) || (new_address < 0 && ip.b_val > 0))) ) {
                throw NotImplementedException("Out of range!?");
            }
            INET_TYPE result;
            result.a_val = ip.a_val;
            result.b_val = new_address;
            result.c_val = ip.c_val;
            return result;
       });
}

void INetFunctions::Add(DataChunk &args, ExpressionState &state, Vector &result) {
    GenericExecutor::ExecuteBinary<INET_TYPE, PrimitiveType<hugeint_t>, INET_TYPE>(
        args.data[0], args.data[1], result, args.size(), [&](INET_TYPE ip, PrimitiveType<hugeint_t> val) {
            if (IPAddressType(ip.a_val) == IPAddressType::IP_ADDRESS_V4) {
                if (val.val > UINT32_MAX) throw NotImplementedException("Out of range!?");
            }
            auto new_address = ip.b_val + val.val;

            if ( ((IPAddressType(ip.a_val) == IPAddressType::IP_ADDRESS_V4) && (new_address < 0 || new_address > UINT32_MAX)) ||
                 ((IPAddressType(ip.a_val) == IPAddressType::IP_ADDRESS_V6) &&
                  ((new_address > 0 && ip.b_val < 0) || (new_address < 0 && ip.b_val > 0))) ) {
                throw NotImplementedException("Out of range!?");
            }
            INET_TYPE result;
            result.a_val = ip.a_val;
            result.b_val = new_address;
            result.c_val = ip.c_val;
            return result;
       });
}

bool INetFunctions::InetContains(INET_TYPE source, INET_TYPE target) {
    if (target.a_val != source.a_val) throw NotImplementedException("IPv4 contains IPv6 or vice-a-versa!?");
    if (target.c_val >= source.c_val) return false;
    if (target.c_val == 0) return true;
    else {
        if (IPAddressType(target.a_val) == IPAddressType::IP_ADDRESS_V4) {
            net::ipaddress &sbuf = *(net::ipaddress*)&source.b_val;
            net::ipaddress &tbuf = *(net::ipaddress*)&target.b_val;
            return ((sbuf.lvalue[0] >> (IPAddress::IPV4_DEFAULT_MASK - target.c_val)) ==
                    (tbuf.lvalue[0] >> (IPAddress::IPV4_DEFAULT_MASK - target.c_val)));
        } else {
            return ((source.b_val >> (IPAddress::IPV6_DEFAULT_MASK - target.c_val)) ==
                    (target.b_val >> (IPAddress::IPV6_DEFAULT_MASK - target.c_val)));
        }
    }
}

bool INetFunctions::InetContainsOrEqual(INET_TYPE source, INET_TYPE target) {
    if (target.a_val != source.a_val) throw NotImplementedException("IPv4 contains IPv6 or vice-a-versa!?");
    if (target.c_val > source.c_val) return false;
    if (target.c_val == 0) return true;
    else {
        if (IPAddressType(target.a_val) == IPAddressType::IP_ADDRESS_V4) {
            net::ipaddress &sbuf = *(net::ipaddress*)&source.b_val;
            net::ipaddress &tbuf = *(net::ipaddress*)&target.b_val;
            return ((sbuf.lvalue[0] >> (IPAddress::IPV4_DEFAULT_MASK - target.c_val)) ==
                    (tbuf.lvalue[0] >> (IPAddress::IPV4_DEFAULT_MASK - target.c_val)));
        } else {
            return ((source.b_val >> (IPAddress::IPV6_DEFAULT_MASK - target.c_val)) ==
                    (target.b_val >> (IPAddress::IPV6_DEFAULT_MASK - target.c_val)));
        }
    }
}

void INetFunctions::Contains(DataChunk &args, ExpressionState &state, Vector &result) {
    GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
        args.data[0], args.data[1], result, args.size(), InetContains);
}

void INetFunctions::ContainsOrEqual(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
	    args.data[0], args.data[1], result, args.size(), InetContainsOrEqual);
}

void INetFunctions::ContainWithin(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
	    args.data[0], args.data[1], result, args.size(), [&](INET_TYPE source, INET_TYPE target) {
            return InetContains(target, source);
	    });
}

void INetFunctions::ContainWithinOrEqual(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
	    args.data[0], args.data[1], result, args.size(), [&](INET_TYPE source, INET_TYPE target) {
            return InetContainsOrEqual(target, source);
	    });
}

bool INetFunctions::InetEqual(const INET_TYPE source, const INET_TYPE target) {
    if (target.a_val != source.a_val) throw NotImplementedException("Comparing IPv4 with IPv6 or vice-a-versa!?");
    if (target.c_val != source.c_val) return false;
    else if (IPAddressType(target.a_val) == IPAddressType::IP_ADDRESS_V4) {
        net::ipaddress &sbuf = *(net::ipaddress*)&source.b_val;
        net::ipaddress &tbuf = *(net::ipaddress*)&target.b_val;
        return (sbuf.lvalue[0] == tbuf.lvalue[0]);
    } else {
        return (source.b_val == target.b_val);
    }
}

void INetFunctions::Equal(DataChunk &args, ExpressionState &state, Vector &result) {
    GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
            args.data[0], args.data[1], result, args.size(), InetEqual);
}

void INetFunctions::NotEqual(DataChunk &args, ExpressionState &state, Vector &result) {
       GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
            args.data[0], args.data[1], result, args.size(), [&](INET_TYPE source, INET_TYPE target) {
            return !InetEqual(source, target);
            });
}

bool INetFunctions::InetLessThan(const INET_TYPE source, const INET_TYPE target) {
    if (target.a_val != source.a_val) throw NotImplementedException("Comparing IPv4 with IPv6 or vice-a-versa!?");
    if (target.c_val > source.c_val) return true;
    else if (target.c_val < source.c_val) return false;
    else {
        if (IPAddressType(target.a_val) == IPAddressType::IP_ADDRESS_V4) {
            net::ipaddress &sbuf = *(net::ipaddress*)&source.b_val;
            net::ipaddress &tbuf = *(net::ipaddress*)&target.b_val;
            return (sbuf.lvalue[0] < tbuf.lvalue[0]);
        } else {
            return (source.b_val < target.b_val);
        }
    }
}

bool INetFunctions::InetLessThanOrEqual(const INET_TYPE source, const INET_TYPE target) {
    if (target.a_val != source.a_val) throw NotImplementedException("Comparing IPv4 with IPv6 or vice-a-versa!?");
    if (target.c_val > source.c_val) return true;
    else if (target.c_val < source.c_val) return false;
    else {
        if (IPAddressType(target.a_val) == IPAddressType::IP_ADDRESS_V4) {
            net::ipaddress &sbuf = *(net::ipaddress*)&source.b_val;
            net::ipaddress &tbuf = *(net::ipaddress*)&target.b_val;
            return (sbuf.lvalue[0] <= tbuf.lvalue[0]);
        } else {
            return (source.b_val <= target.b_val);
        }
    }
}

void INetFunctions::LessThan(DataChunk &args, ExpressionState &state, Vector &result) {
    GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
            args.data[0], args.data[1], result, args.size(), InetLessThan);
}

void INetFunctions::LessThanOrEqual(DataChunk &args, ExpressionState &state, Vector &result) {
    GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
            args.data[0], args.data[1], result, args.size(), InetLessThanOrEqual);
}

void INetFunctions::GreaterThan(DataChunk &args, ExpressionState &state, Vector &result) {
    GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
            args.data[0], args.data[1], result, args.size(), [&](INET_TYPE source, INET_TYPE target) {
            return !InetLessThanOrEqual(source, target);
            });
}

void INetFunctions::GreaterThanOrEqual(DataChunk &args, ExpressionState &state, Vector &result) {
    GenericExecutor::ExecuteBinary<INET_TYPE, INET_TYPE, PrimitiveType<bool>>(
            args.data[0], args.data[1], result, args.size(), [&](INET_TYPE source, INET_TYPE target) {
            return !InetLessThan(source, target);
            });
}

} // namespace duckdb
