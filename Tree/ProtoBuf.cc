/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Core/Debug.h"
 
#include "Core/ProtoBuf.h"
#include "Tree/ProtoBuf.h"

namespace LogCabin {
namespace Tree {
namespace ProtoBuf {

namespace PC = LogCabin::Protocol::Client;

void
readOnlyTreeRPC(Tree& tree,
                const PC::ReadOnlyTree::Request& request,
                PC::ReadOnlyTree::Response& response)
{
    Result result;
    if (request.has_condition()) {
        result = tree.checkCondition(request.condition().path(),
                                     request.condition().contents());
    }
    if (result.status != Status::OK) {
        // condition does not match, skip
    } else if (request.has_list_directory()) {
        std::vector<std::string> children;
        result = tree.listDirectory(request.list_directory().path(),
                                    children);
        for (auto it = children.begin(); it != children.end(); ++it)
            response.mutable_list_directory()->add_child(*it);
    } else if (request.has_read()) {
        std::string contents;
        result = tree.read(
                request.read().path(),
                contents);
        response.mutable_read()->set_contents(contents);
    } else if (request.has_head()) {
        std::string contents;
        result = tree.head(request.head().path(), contents);
        response.mutable_read()->set_contents(contents);
    } else if (request.has_lrange()) {
        std::string ouput;
        result = tree.lrange(request.lrange().path(), request.lrange().args(), ouput);
        response.mutable_read()->set_contents(ouput);
    } else {
        PANIC("Unexpected request: %s",
              Core::ProtoBuf::dumpString(request).c_str());
    }
    response.set_status(static_cast<PC::Status>(result.status));
    if (result.status != Status::OK)
        response.set_error(result.error);
}

void
readWriteTreeRPC(Tree& tree,
                 const PC::ReadWriteTree::Request& request,
                 PC::ReadWriteTree::Response& response)
{
    Result result;
    std::string contents;
    if (request.has_condition()) {
        result = tree.checkCondition(request.condition().path(),
                                     request.condition().contents());
    }
    if (result.status != Status::OK) {
        // condition does not match, skip
    } else if (request.has_rpush()) {
        result = tree.rpush(request.rpush().path(),
                            request.rpush().contents(),
                            request.request_time());
    } else if (request.has_lpop()) {
        result = tree.lpop(request.lpop().path(), contents, request.request_time());
        response.set_error(contents);
    } else if (request.has_lrem()) {
        result = tree.lrem(request.lrem().path(),
                           request.lrem().contents(), request.request_time());
    } else if (request.has_make_directory()) {
        result = tree.makeDirectory(request.make_directory().path());
    } else if (request.has_remove_directory()) {
        result = tree.removeDirectory(request.remove_directory().path());
    } else if (request.has_write()) {
        result = tree.write(request.write().path(),
                            request.write().contents(),
                            request.request_time());
    } else if (request.has_remove_file()) {
        result = tree.removeFile(request.remove_file().path());
    } else if (request.has_sadd()) {
        result = tree.sadd(request.sadd().path(),
                           request.sadd().contents());
    } else if (request.has_srem()) {
        result = tree.srem(request.srem().path(),
                           request.srem().contents());
    } else if (request.has_pub()) {
        result = tree.pub(request.pub().path(),
                          request.pub().contents());
    } else if (request.has_ltrim()) {
        result = tree.ltrim(request.ltrim().path(),
                          request.ltrim().contents(), request.request_time());
    } else if (request.has_expire()) {
        //don't panic, it's a pure expire request
    } else {
        PANIC("Unexpected request: %s",
              Core::ProtoBuf::dumpString(request).c_str());
    }
    //handle expire after other writtings
    if (request.has_expire()) {
        uint32_t operation = 0;
        if(request.expire().has_operation())
        {
            operation = request.expire().operation();
        }else
        {
            operation = Protocol::Client::ExpireOpCode::SET_UP_EXPIRE_IN;
        }
        result = tree.expire(request.expire().path(),
                request.expire().contents(),
                operation,
                request.request_time()
                );
    }
    response.set_status(static_cast<PC::Status>(result.status));
    if (result.status != Status::OK)
        response.set_error(result.error);
}

} // namespace LogCabin::Tree::ProtoBuf
} // namespace LogCabin::Tree
} // namespace LogCabin
