#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE PROTOCOL

#include <boost/test/unit_test.hpp>
#include "redis_protocol/resp_protocol.hpp"

using namespace rediscpp::protocol;

namespace rediscpp {
    namespace protocol {

        std::ostream& operator<< (std::ostream& s, EncodeDecodeResult e)
        {
            switch (e) {
                case EncodeDecodeResult::OK:
                    s << "Encode-Decode successful";
                    break;
                case EncodeDecodeResult::NIL:
                    s << "Encode-Decode returned NIL object";
                    break;
                case EncodeDecodeResult::PARSE_ERROR:
                    s << "Encode-Decode  failed";
                    break;
            }
            return s;
        }
    }
}
/* ============================================================================
    TEST ENCODING METHODS.
============================================================================ */

BOOST_AUTO_TEST_CASE(encode_int)
{
    std::string packet;
    EncodeDecodeResult ret = EncodeInteger(5, packet);

    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, ":5\r\n");

    ret = EncodeInteger(11115, packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, ":11115\r\n");

    ret = EncodeInteger(0, packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, ":0\r\n");

    ret = EncodeInteger(-10, packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, ":-10\r\n");

    ret = EncodeInteger(-1555448645, packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, ":-1555448645\r\n");
}

BOOST_AUTO_TEST_CASE(encode_string)
{
    std::string packet;

    EncodeDecodeResult ret = EncodeString("my string", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "+my string\r\n");

    ret = EncodeString("OK", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "+OK\r\n");

    ret = EncodeString("", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "+\r\n");
}

BOOST_AUTO_TEST_CASE(encode_error)
{
    std::string packet;

    EncodeDecodeResult ret = EncodeError("my string", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "-my string\r\n");

    ret = EncodeError("OK", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "-OK\r\n");

    ret = EncodeError("", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "-\r\n");
}

BOOST_AUTO_TEST_CASE(encode_bulk_string)
{
    std::string packet;

    EncodeDecodeResult ret = EncodeBulkString("", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "$0\r\n\r\n");

    ret = EncodeBulkString("hi", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "$2\r\nhi\r\n");

    ret = EncodeBulkString("hello\r\nhi", packet);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "$9\r\nhello\r\nhi\r\n");
}


BOOST_AUTO_TEST_CASE(encode_array)
{
    std::vector<std::string> array = {"hi", "", "hello\r\n2"};
    std::string packet;
    EncodeDecodeResult ret = EncodeBulkStringArray(array, packet);

    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(packet, "*3\r\n$2\r\nhi\r\n$0\r\n\r\n$8\r\nhello\r\n2\r\n");
}

/* ============================================================================
    TEST ENCODING METHODS.
============================================================================ */

BOOST_AUTO_TEST_CASE(decode_int)
{
    int integer;
    EncodeDecodeResult ret = DecodeInteger(":0\r\n", integer);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(integer, 0);

    DecodeInteger(":-1000\r\n", integer);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(integer, -1000);

    DecodeInteger(":4258\r\n", integer);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(integer, 4258);

    // Now, bad values.
    ret = DecodeInteger("foefw;ef", integer);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::PARSE_ERROR);

    ret = DecodeInteger(":aaa\r\n", integer);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::PARSE_ERROR);

    ret = DecodeInteger(":2", integer);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::PARSE_ERROR);
}

BOOST_AUTO_TEST_CASE(decode_string)
{
    std::string result;
    EncodeDecodeResult ret = DecodeString("+OK\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(result, "OK");

    DecodeString("+\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(result, "");

    DecodeString("+OK ca marche\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(result, "OK ca marche");

    // Failure
    ret = DecodeString("-\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::PARSE_ERROR);

    ret = DecodeString("+hi\r\n\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::PARSE_ERROR);
}

BOOST_AUTO_TEST_CASE(decode_bulk_string)
{
    std::string result;
    EncodeDecodeResult ret = DecodeBulkString("$5\r\nhello\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(result, "hello");

    ret = DecodeBulkString("$0\r\n\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(result, "");

    ret = DecodeBulkString("$8\r\nhello\r\no\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(result, "hello\r\no");

    ret = DecodeBulkString("$10\r\nhe\r\nhe\r\nhe\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::OK);
    BOOST_CHECK_EQUAL(result, "he\r\nhe\r\nhe");

    // NIL BULK string
    ret = DecodeBulkString("$-1\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::NIL);

    // Failures
    ret = DecodeBulkString("%d21d", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::PARSE_ERROR);

    // too short
    ret = DecodeBulkString("$5\r\nhi\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::PARSE_ERROR);

    // too long
    ret = DecodeBulkString("$5\r\nhiiiiiiiiiiii\r\n", result);
    BOOST_CHECK_EQUAL(ret, EncodeDecodeResult::PARSE_ERROR);
}
