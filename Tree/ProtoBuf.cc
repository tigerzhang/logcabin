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
    } 
    else
    {
        switch(request.command())
        {
            case Protocol::Client::LISD_DIRECTORY:
                {

                    std::vector<std::string> children;
                    result = tree.listDirectory(request.path(),
                            children);
                    for (auto it = children.begin(); it != children.end(); ++it)
                        response.add_elem(*it);
                    break;
                }
            case Protocol::Client::READ:
                {

                    std::string contents;
                    result = tree.read(
                            request.path(),
                            contents);
                    response.set_content(contents);
                    break;
                }
            case Protocol::Client::HEAD:
                {

                    std::string contents;
                    result = tree.head(request.path(), contents);
                    response.set_content(contents);
                    break;
                }
            case Protocol::Client::LRANGE:
                {
                    std::vector<std::string> output;
                    result = tree.lrange(request.path(), request.args(), output);
                    for (auto it = output.begin(); it != output.end(); ++it)
                        response.add_elem(*it);
                    break;
                }
            case Protocol::Client::SMEMBERS:
                {
                    std::vector<std::string> output;
                    result = tree.smembers(request.path(), output);
                    for (auto it = output.begin(); it != output.end(); ++it)
                        response.add_elem(*it);
                    break;
                }
            case Protocol::Client::SCARD:
                {
                    std::string output;
                    result = tree.scard(request.path(), output);
                    response.set_content(output);
                    break;
                }
            default:
                PANIC("Unexpected request: %s",
                        Core::ProtoBuf::dumpString(request).c_str());
                break;
        }

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
    } 
    else
    {
        switch(request.command())
        {
            case Protocol::Client::RPUSH:
                {
                    result = tree.rpush(request.path(),
                            request.contents(),
                            request.request_time());
                    break;
                }
            case Protocol::Client::LPUSH:
                {
                    result = tree.lpush(request.path(),
                            request.contents(),
                            request.request_time());
                    break;
                }
            case Protocol::Client::LPOP:
                {
                    result = tree.lpop(request.path(),
                            request.contents(),
                            request.request_time());
                    response.set_error(contents);
                    break;
                }
            case Protocol::Client::LREM:
                {
                    result = tree.lrem(request.path(),
                            request.contents(),
                            request.count(),
                            request.request_time());
                    break;
                }
            case Protocol::Client::MAKE_DIRECTORY:
                {
                    result = tree.makeDirectory(request.path());
                    break;
                }
            case Protocol::Client::REMOVE_DIRECTORY:
                {
                    result = tree.removeDirectory(request.path());
                    break;
                }
            case Protocol::Client::WRITE:
                {
                    result = tree.write(request.path(),
                            request.contents(),
                            request.request_time());
                    break;
                }
            case Protocol::Client::REMOVE_FILE:
                {
                    result = tree.removeFile(request.path());
                    break;
                }
            case Protocol::Client::SADD:
                {
                    std::vector<std::string> args;
                    args.resize((int)(request.args().size()));
                    for(unsigned long i = 0 ; i < request.args().size(); i++)
                    {
                        args[i] = request.args(i);
                    }
                    result = tree.sadd(request.path(),
                            args);
                    break;
                }
            case Protocol::Client::SREM:
                {
                    result = tree.srem(request.path(),
                            request.contents());
                    break;
                }
            case Protocol::Client::PUB:
                {
                    result = tree.pub(request.path(),
                            request.contents());
                    break;
                }
            case Protocol::Client::LTRIM:
                {
                    result = tree.ltrim(request.path(),
                            request.contents(), request.request_time());
                    break;
                }
            case Protocol::Client::SET_UP_EXPIRE_IN:
            case Protocol::Client::CLEAN_UP_EXPIRE_KEYS:
                break;
            default:
                PANIC("Unexpected request: %s",
                        Core::ProtoBuf::dumpString(request).c_str());
                break;

                
        }
    }

    response.set_status(static_cast<PC::Status>(result.status));
    if (result.status != Status::OK)
        response.set_error(result.error);
}

} // namespace LogCabin::Tree::ProtoBuf
} // namespace LogCabin::Tree
} // namespace LogCabin
