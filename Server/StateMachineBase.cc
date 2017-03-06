/* Copyright (c) 2012-2014 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <rocksdb/status.h>
#include <Client.pb.h>
#include <SnapshotStateMachine.pb.h>
#include <include/LogCabin/Client.h>
#include <ServerStats.pb.h>

#include "Core/Debug.h"
#include "Core/Mutex.h"
#include "Core/ProtoBuf.h"
#include "Core/Random.h"
#include "Core/ThreadId.h"
#include "Core/Util.h"
#include "Server/Globals.h"
#include "Tree/ProtoBuf.h"
#include "Server/StateMachineRocksdb.h"

namespace LogCabin {
namespace Server {

namespace PC = LogCabin::Protocol::Client;


// for testing purposes
extern bool stateMachineSuppressThreads;
extern uint32_t stateMachineChildSleepMs;

StateMachineBase::StateMachineBase(std::shared_ptr<RaftConsensus> consensus,
                           Core::Config& config,
                           Globals& globals, void *kvstore_)
    : consensus(consensus)
    , globals(globals)
      // This configuration option isn't advertised as part of the public API:
      // it's only useful for testing.
    , snapshotBlockPercentage(
            config.read<uint64_t>("snapshotBlockPercentage", 0))
    , snapshotMinLogSize(
            config.read<uint64_t>("snapshotMinLogSize", 64UL * 1024 * 1024))
    , snapshotRatio(
            config.read<uint64_t>("snapshotRatio", 4))
    , snapshotWatchdogInterval(std::chrono::milliseconds(
            config.read<uint64_t>("snapshotWatchdogMilliseconds", 10000)))
      // TODO(ongaro): This should be configurable, but it must be the same for
      // every server, so it's dangerous to put it in the config file. Need to
      // use the Raft log to agree on this value. Also need to inform clients
      // of the value and its changes, so that they can send keep-alives at
      // appropriate intervals. For now, servers time out after about an hour,
      // and clients send keep-alives every minute.
    , sessionTimeoutNanos(1000UL * 1000 * 1000 * 60 * 60)
    , unknownRequestMessageBackoff(std::chrono::milliseconds(
            config.read<uint64_t>("stateMachineUnknownRequestMessage"
                                  "BackoffMilliseconds", 10000)))
    , mutex()
    , entriesApplied()
    , snapshotSuggested()
    , snapshotStarted()
    , snapshotCompleted()
    , exiting(false)
    , childPid(0)
    , lastApplied(0)
    , lastUnknownRequestMessage(TimePoint::min())
    , numUnknownRequests(0)
    , numUnknownRequestsSinceLastMessage(0)
    , numSnapshotsAttempted(0)
    , numSnapshotsFailed(0)
    , numRedundantAdvanceVersionEntries(0)
    , numRejectedAdvanceVersionEntries(0)
    , numSuccessfulAdvanceVersionEntries(0)
    , numTotalAdvanceVersionEntries(0)
    , isSnapshotRequested(false)
    , maySnapshotAt(TimePoint::min())
    , sessions()
    , tree()
    , versionHistory()
    , writer()
    , applyThread()
    , snapshotThread()
    , snapshotWatchdogThread()
    , _options()
{
    versionHistory.insert({0, 1});
    consensus->setSupportedStateMachineVersions(MIN_SUPPORTED_VERSION,
                                                MAX_SUPPORTED_VERSION);

    kvstore = kvstore_;
//    initKVStore();

    if (!stateMachineSuppressThreads) {
        applyThread = std::thread(&StateMachineBase::applyThreadMain, this);
        snapshotThread = std::thread(&StateMachineBase::snapshotThreadMain, this);
        snapshotWatchdogThread = std::thread(
                &StateMachineBase::snapshotWatchdogThreadMain, this);
    }
}

StateMachineBase::~StateMachineBase()
{
    NOTICE("Shutting down");
    if (consensus) // sometimes missing for testing
        consensus->exit();
    if (applyThread.joinable())
        applyThread.join();
    if (snapshotThread.joinable())
        snapshotThread.join();
    if (snapshotWatchdogThread.joinable())
        snapshotWatchdogThread.join();
    NOTICE("Joined with threads");

//    if (kvstore) delete(kvstore);
}

bool
StateMachineBase::query(const Query::Request& request,
                    Query::Response& response) const
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
//    if (request.has_tree()) {
//        Tree::ProtoBuf::readOnlyTreeRPC(tree,
//                                        request.tree(),
//                                        *response.mutable_tree());
//        return true;
//    }
    VERBOSE("query: %s\n",
            Core::StringUtil::trim(Core::ProtoBuf::dumpString(request)).c_str());
    if (request.has_key_value()) {
        const PC::ReadOnlyKeyValue::Request& keyValue = request.key_value();
        std::string contents;
        int result = get(keyValue.key(), &contents);
        if (result == 0) {
            response.mutable_key_value()->set_value(contents);
            response.mutable_key_value()->set_status(Protocol::Client::Status::OK);
            return true;
        } else {
            response.mutable_key_value()->set_error(contents);
            response.mutable_key_value()->set_status(Protocol::Client::Status::LOOKUP_ERROR);
            return true;
        }
    } else if (request.has_tree()) {
        const PC::ReadOnlyTree::Request &tree = request.tree();
        if (tree.has_read()) {
            std::string contents;
            int result = get(tree.read().path(), &contents);
            if (result == 0) {
                response.mutable_tree()->mutable_read()->set_contents(contents);
                response.mutable_tree()->set_status(Protocol::Client::Status::OK);
                return true;
            } else {
                response.mutable_tree()->set_error("TODO error message");
                response.mutable_tree()->set_status(Protocol::Client::Status::LOOKUP_ERROR);
                return true;
            }
        } else {
            response.mutable_tree()->set_error("Only read operation supported");
            response.mutable_tree()->set_status(Protocol::Client::Status::TYPE_ERROR);
            return true;
        }
    }
    warnUnknownRequest(request, "does not understand the given request");
    return false;
}

void
StateMachineBase::updateServerStats(Protocol::ServerStats& serverStats) const
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    Core::Time::SteadyTimeConverter time;
    serverStats.clear_state_machine();
    Protocol::ServerStats::StateMachine& smStats =
        *serverStats.mutable_state_machine();
    smStats.set_snapshotting(childPid != 0);
    smStats.set_last_applied(lastApplied);
    smStats.set_num_sessions(sessions.size());
    smStats.set_num_unknown_requests(numUnknownRequests);
    smStats.set_num_snapshots_attempted(numSnapshotsAttempted);
    smStats.set_num_snapshots_failed(numSnapshotsFailed);
    smStats.set_num_redundant_advance_version_entries(
        numRedundantAdvanceVersionEntries);
    smStats.set_num_rejected_advance_version_entries(
        numRejectedAdvanceVersionEntries);
    smStats.set_num_successful_advance_version_entries(
        numSuccessfulAdvanceVersionEntries);
    smStats.set_num_total_advance_version_entries(
        numTotalAdvanceVersionEntries);
    smStats.set_min_supported_version(MIN_SUPPORTED_VERSION);
    smStats.set_max_supported_version(MAX_SUPPORTED_VERSION);
    smStats.set_running_version(getVersion(lastApplied));
    smStats.set_may_snapshot_at(time.unixNanos(maySnapshotAt));
    tree.updateServerStats(*smStats.mutable_tree());
}

void
StateMachineBase::wait(uint64_t index) const
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    while (lastApplied < index)
        entriesApplied.wait(lockGuard);
}

bool
StateMachineBase::waitForResponse(uint64_t logIndex,
                              const Command::Request& command,
                              Command::Response& response) const
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    while (lastApplied < logIndex)
        entriesApplied.wait(lockGuard);

    // Need to check whether we understood the request at the time it
    // was applied using getVersion(logIndex), then reply and return true/false
    // based on that. Existing commands have been around since version 1, so we
    // skip this check for now.
    uint16_t versionThen = getVersion(logIndex);

    if (command.has_key_value()) {
        response.mutable_key_value()->set_status(PC::Status::OK);
        return true;
    } else if (command.has_tree()) {
        const PC::ExactlyOnceRPCInfo& rpcInfo = command.tree().exactly_once();
        auto sessionIt = sessions.find(rpcInfo.client_id());
        if (sessionIt == sessions.end()) {
            WARNING("Client %lu session expired but client still active",
                    rpcInfo.client_id());
            response.mutable_tree()->
                set_status(PC::Status::SESSION_EXPIRED);
            return true;
        }
        const Session& session = sessionIt->second;
        auto responseIt = session.responses.find(rpcInfo.rpc_number());
        if (responseIt == session.responses.end()) {
            // The response for this RPC has already been removed: the client
            // is not waiting for it. This request is just a duplicate that is
            // safe to drop.
            WARNING("Client %lu asking for discarded response to RPC %lu",
                    rpcInfo.client_id(), rpcInfo.rpc_number());
            response.mutable_tree()->
                set_status(PC::Status::SESSION_EXPIRED);
            return true;
        }
        response = responseIt->second;
        return true;
    } else if (command.has_open_session()) {
        response.mutable_open_session()->
            set_client_id(logIndex);
        return true;
    } else if (versionThen >= 2 && command.has_close_session()) {
        response.mutable_close_session(); // no fields to set
        return true;
    } else if (command.has_advance_version()) {
        response.mutable_advance_version()->
            set_running_version(versionThen);
        return true;
    }
    // don't warnUnknownRequest here, since we already did so in apply()
    return false;
}

bool
StateMachineBase::isTakingSnapshot() const
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    return childPid != 0;
}

void
StateMachineBase::startTakingSnapshot()
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    if (childPid == 0) {
        NOTICE("Administrator requested snapshot");
        isSnapshotRequested = true;
        snapshotSuggested.notify_all();
        // This waits on numSnapshotsAttempted to change, since waiting on
        // childPid != 0 would risk missing an entire snapshot that started and
        // completed before this thread was scheduled.
        uint64_t nextSnapshot = numSnapshotsAttempted + 1;
        while (!exiting && numSnapshotsAttempted < nextSnapshot) {
            snapshotStarted.wait(lockGuard);
        }
    }
}

void
StateMachineBase::stopTakingSnapshot()
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    int pid = childPid;
    if (pid != 0) {
        NOTICE("Administrator aborted snapshot");
        killSnapshotProcess(Core::HoldingMutex(lockGuard), SIGTERM);
        while (!exiting && pid == childPid) {
            snapshotCompleted.wait(lockGuard);
        }
    }
}

std::chrono::nanoseconds
StateMachineBase::getInhibit() const
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    TimePoint now = Clock::now();
    if (maySnapshotAt <= now) {
        return std::chrono::nanoseconds::zero();
    } else {
        return (std::chrono::nanoseconds)(maySnapshotAt - now);
    }
}

void
StateMachineBase::setInhibit(std::chrono::nanoseconds duration)
{
    std::lock_guard<Core::Mutex> lockGuard(mutex);
    if (duration <= std::chrono::nanoseconds::zero()) {
        maySnapshotAt = TimePoint::min();
        NOTICE("Administrator permitted snapshotting");
    } else {
        TimePoint now = Clock::now();
        maySnapshotAt = now + duration;
        if (maySnapshotAt < now) { // overflow
            maySnapshotAt = TimePoint::max();
        }
        NOTICE("Administrator inhibited snapshotting for the next %s",
               Core::StringUtil::toString(maySnapshotAt - now).c_str());
    }
    snapshotSuggested.notify_all();
}


////////// StateMachineBase private methods //////////

void
StateMachineBase::apply(const RaftConsensus::Entry& entry)
{
    Command::Request command;
    if (!Core::ProtoBuf::parse(entry.command, command)) {
        PANIC("Failed to parse protobuf for entry %lu",
              entry.index);
    }
    uint16_t runningVersion = getVersion(entry.index - 1);
    if (command.has_key_value()) {
        const PC::ReadWriteKeyValue::Request &request = command.key_value();
        int ret = put(request.key(), request.value());
        assert(ret == 0);
    } else if (command.has_tree()) {
        PC::ExactlyOnceRPCInfo rpcInfo = command.tree().exactly_once();
        auto it = sessions.find(rpcInfo.client_id());
        if (it == sessions.end()) {
            // session does not exist
        } else {
            // session exists
            Session& session = it->second;
            expireResponses(session, rpcInfo.first_outstanding_rpc());
            if (rpcInfo.rpc_number() < session.firstOutstandingRPC) {
                // response already discarded, do not re-apply
            } else {
                auto inserted = session.responses.insert(
                                                {rpcInfo.rpc_number(), {}});
                if (inserted.second) {
                    // response not found, apply and save it
                    Tree::ProtoBuf::readWriteTreeRPC(
                        tree,
                        command.tree(),
                        *inserted.first->second.mutable_tree());
                    session.lastModified = entry.clusterTime;

                    const PC::ReadWriteTree::Request req = command.tree();
                    if (req.has_write()) {
                        const std::string path = req.write().path();
                        const std::string content = req.write().contents();
                        VVERBOSE("apply %s:%s", path.c_str(), content.c_str());
                        int result = put(path, content);
                        assert(result == 0);
                    }
                } else {
                    // response exists, do not re-apply
                }
            }
        }
    } else if (command.has_open_session()) {
        uint64_t clientId = entry.index;
        Session& session = sessions.insert({clientId, {}}).first->second;
        session.lastModified = entry.clusterTime;
    } else if (command.has_close_session()) {
        if (runningVersion >= 2) {
            sessions.erase(command.close_session().client_id());
        } else {
            // Command is ignored in version < 2.
            warnUnknownRequest(command, "may not process the given request, "
                               "which was introduced in version 2");
        }
    } else if (command.has_advance_version()) {
        uint16_t requested = Core::Util::downCast<uint16_t>(
                command.advance_version(). requested_version());
        if (requested < runningVersion) {
            WARNING("Rejecting downgrade of state machine version "
                    "(running version %u but command at log index %lu wants "
                    "to switch to version %u)",
                    runningVersion,
                    entry.index,
                    requested);
            ++numRejectedAdvanceVersionEntries;
        } else if (requested > runningVersion) {
            if (requested > MAX_SUPPORTED_VERSION) {
                PANIC("Cannot upgrade state machine to version %u (from %u) "
                      "because this code only supports up to version %u",
                      requested,
                      runningVersion,
                      MAX_SUPPORTED_VERSION);
            } else {
                NOTICE("Upgrading state machine to version %u (from %u)",
                       requested,
                       runningVersion);
                versionHistory.insert({entry.index, requested});
            }
            ++numSuccessfulAdvanceVersionEntries;
        } else { // requested == runningVersion
            // nothing to do
            // If this stat is high, see note in RaftConsensus.cc.
            ++numRedundantAdvanceVersionEntries;
        }
        ++numTotalAdvanceVersionEntries;
    } else { // unknown command
        // This is (deterministically) ignored by all state machines running
        // the current version.
        warnUnknownRequest(command, "does not understand the given request");
    }
}

void
StateMachineBase::applyThreadMain()
{
    Core::ThreadId::setName("StateMachineBase");
    try {
        while (true) {
            RaftConsensus::Entry entry = consensus->getNextEntry(lastApplied);
            std::lock_guard<Core::Mutex> lockGuard(mutex);
            switch (entry.type) {
                case RaftConsensus::Entry::SKIP:
                    break;
                case RaftConsensus::Entry::DATA:
                    apply(entry);
                    break;
                case RaftConsensus::Entry::SNAPSHOT:
                    NOTICE("Loading snapshot through entry %lu into state "
                           "machine", entry.index);
                    loadSnapshot(*entry.snapshotReader);
                    NOTICE("Done loading snapshot");
                    break;
            }
            expireSessions(entry.clusterTime);
            lastApplied = entry.index;
            entriesApplied.notify_all();
            if (shouldTakeSnapshot(lastApplied) &&
                maySnapshotAt <= Clock::now()) {
                snapshotSuggested.notify_all();
            }
        }
    } catch (const Core::Util::ThreadInterruptedException&) {
        NOTICE("exiting");
        std::lock_guard<Core::Mutex> lockGuard(mutex);
        exiting = true;
        entriesApplied.notify_all();
        snapshotSuggested.notify_all();
        snapshotStarted.notify_all();
        snapshotCompleted.notify_all();
        killSnapshotProcess(Core::HoldingMutex(lockGuard), SIGTERM);
    }
}

void
StateMachineBase::serializeSessions(SnapshotStateMachine::Header& header) const
{
    for (auto it = sessions.begin(); it != sessions.end(); ++it) {
        SnapshotStateMachine::Session& session = *header.add_session();
        session.set_client_id(it->first);
        session.set_last_modified(it->second.lastModified);
        session.set_first_outstanding_rpc(it->second.firstOutstandingRPC);
        for (auto it2 = it->second.responses.begin();
             it2 != it->second.responses.end();
             ++it2) {
            SnapshotStateMachine::Response& response =
                *session.add_rpc_response();
            response.set_rpc_number(it2->first);
            *response.mutable_response() = it2->second;
        }
    }
}

void
StateMachineBase::expireResponses(Session& session, uint64_t firstOutstandingRPC)
{
    if (session.firstOutstandingRPC >= firstOutstandingRPC)
        return;
    session.firstOutstandingRPC = firstOutstandingRPC;
    auto it = session.responses.begin();
    while (it != session.responses.end()) {
        if (it->first < session.firstOutstandingRPC)
            it = session.responses.erase(it);
        else
            ++it;
    }
}

void
StateMachineBase::expireSessions(uint64_t clusterTime)
{
    auto it = sessions.begin();
    while (it != sessions.end()) {
        Session& session = it->second;
        uint64_t expireTime = session.lastModified + sessionTimeoutNanos;
        if (expireTime < clusterTime) {
            uint64_t diffNanos = clusterTime - session.lastModified;
            NOTICE("Expiring client %lu's session after %lu.%09lu seconds "
                   "of cluster time due to inactivity",
                   it->first,
                   diffNanos / (1000 * 1000 * 1000UL),
                   diffNanos % (1000 * 1000 * 1000UL));
            it = sessions.erase(it);
        } else {
            ++it;
        }
    }
}

uint16_t
StateMachineBase::getVersion(uint64_t logIndex) const
{
    auto it = versionHistory.upper_bound(logIndex);
    --it;
    return it->second;
}

void
StateMachineBase::killSnapshotProcess(Core::HoldingMutex holdingMutex,
                                  int signum)
{
    if (childPid != 0) {
        int r = kill(childPid, signum);
        if (r != 0) {
            WARNING("Could not send %s to child process (%d): %s",
                    strsignal(signum),
                    childPid,
                    strerror(errno));
        }
    }
}

void
StateMachineBase::loadSessions(const SnapshotStateMachine::Header& header)
{
    sessions.clear();
    for (auto it = header.session().begin();
         it != header.session().end();
         ++it) {
        Session& session = sessions.insert({it->client_id(), {}})
                                                        .first->second;
        session.lastModified = it->last_modified();
        session.firstOutstandingRPC = it->first_outstanding_rpc();
        for (auto it2 = it->rpc_response().begin();
             it2 != it->rpc_response().end();
             ++it2) {
            session.responses.insert({it2->rpc_number(), it2->response()});
        }
    }
}

void
StateMachineBase::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
    // Check that this snapshot uses format version 1
    uint8_t formatVersion = 0;
    uint64_t bytesRead = stream.readRaw(&formatVersion, sizeof(formatVersion));
    if (bytesRead < sizeof(formatVersion)) {
        PANIC("Snapshot contents are empty (no format version field)");
    }
    if (formatVersion != 1) {
        PANIC("Snapshot contents format version read was %u, but this "
              "code can only read version 1",
              formatVersion);
    }

    // Load snapshot header
    {
        SnapshotStateMachine::Header header;
        std::string error = stream.readMessage(header);
        if (!error.empty()) {
            PANIC("Couldn't read state machine header from snapshot: %s",
                  error.c_str());
        }
        loadVersionHistory(header);
        loadSessions(header);
    }

    // Load the tree's state
//    tree.loadSnapshot(stream);
    loadSnapshotLoadData(stream);
}

void
StateMachineBase::loadVersionHistory(const SnapshotStateMachine::Header& header)
{
    versionHistory.clear();
    versionHistory.insert({0, 1});
    for (auto it = header.version_update().begin();
         it != header.version_update().end();
         ++it) {
        versionHistory.insert({it->log_index(),
                               Core::Util::downCast<uint16_t>(it->version())});
    }

    // The version of the current state machine behavior.
    uint16_t running = versionHistory.rbegin()->second;
    if (running < MIN_SUPPORTED_VERSION ||
        running > MAX_SUPPORTED_VERSION) {
        PANIC("State machine version read from snapshot was %u, but this "
              "code only supports %u through %u (inclusive)",
              running,
              MIN_SUPPORTED_VERSION,
              MAX_SUPPORTED_VERSION);
    }
}

void
StateMachineBase::serializeVersionHistory(
        SnapshotStateMachine::Header& header) const
{
    for (auto it = versionHistory.begin();
         it != versionHistory.end();
         ++it) {
        SnapshotStateMachine::VersionUpdate& update =
            *header.add_version_update();
        update.set_log_index(it->first);
        update.set_version(it->second);
    }
}

bool
StateMachineBase::shouldTakeSnapshot(uint64_t lastIncludedIndex) const
{
    SnapshotStats::SnapshotStats stats = consensus->getSnapshotStats();

    // print every 10% but not at 100% because then we'd be printing all the
    // time
    uint64_t curr = 0;
    if (lastIncludedIndex > stats.last_snapshot_index())
        curr = lastIncludedIndex - stats.last_snapshot_index();
    uint64_t prev = curr - 1;
    uint64_t logEntries = stats.last_log_index() - stats.last_snapshot_index();
    if (curr != logEntries &&
        10 * prev / logEntries != 10 * curr / logEntries) {
        NOTICE("Have applied %lu%% of the %lu total log entries",
               100 * curr / logEntries,
               logEntries);
    }

    if (stats.log_bytes() < snapshotMinLogSize)
        return false;
    if (stats.log_bytes() < stats.last_snapshot_bytes() * snapshotRatio)
        return false;
    if (lastIncludedIndex < stats.last_snapshot_index())
        return false;
    if (lastIncludedIndex < stats.last_log_index() * 3 / 4)
        return false;
    return true;
}

void
StateMachineBase::snapshotThreadMain()
{
    Core::ThreadId::setName("SnapshotStateMachine");
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    bool wasInhibited = false;
    while (!exiting) {
        bool inhibited = (maySnapshotAt > Clock::now());

        TimePoint waitUntil = TimePoint::max();
        if (inhibited)
            waitUntil = maySnapshotAt;

        if (wasInhibited && !inhibited)
            NOTICE("Now permitted to take snapshots");
        wasInhibited = inhibited;

        if (isSnapshotRequested ||
            (!inhibited && shouldTakeSnapshot(lastApplied))) {

            isSnapshotRequested = false;
            takeSnapshot(lastApplied, lockGuard);
            continue;
        }

        snapshotSuggested.wait_until(lockGuard, waitUntil);
    }
}

void
StateMachineBase::snapshotWatchdogThreadMain()
{
    using Core::StringUtil::toString;
    Core::ThreadId::setName("SnapshotStateMachineWatchdog");
    std::unique_lock<Core::Mutex> lockGuard(mutex);

    // The snapshot process that this thread is currently tracking, based on
    // numSnapshotsAttempted. If set to ~0UL, this thread is not currently
    // tracking a snapshot process.
    uint64_t tracking = ~0UL;
    // The value of writer->sharedBytesWritten at the "start" time.
    uint64_t startProgress = 0;
    // The time at the "start" time.
    TimePoint startTime = TimePoint::min();
    // Special value for infinite interval.
    const std::chrono::nanoseconds zero = std::chrono::nanoseconds::zero();

    while (!exiting) {
        TimePoint waitUntil = TimePoint::max();
        TimePoint now = Clock::now();

        if (childPid > 0) { // there is some child process
            uint64_t currentProgress = *writer->sharedBytesWritten.value;
            if (tracking == numSnapshotsAttempted) { // tracking current child
                if (snapshotWatchdogInterval != zero &&
                    now >= startTime + snapshotWatchdogInterval) { // check
                    if (currentProgress == startProgress) {
                        ERROR("Snapshot process (counter %lu, pid %u) made no "
                              "progress for %s. Killing it. If this happens "
                              "at all often, you should file a bug to "
                              "understand the root cause.",
                              numSnapshotsAttempted,
                              childPid,
                              toString(snapshotWatchdogInterval).c_str());
                        killSnapshotProcess(Core::HoldingMutex(lockGuard),
                                            SIGKILL);
                        // Don't kill for another interval,
                        // hopefully child will be reaped by then.
                    }
                    startProgress = currentProgress;
                    startTime = now;
                } else {
                    // woke up too early, nothing to do
                }
            } else { // not yet tracking this child
                VERBOSE("Beginning to track snapshot process "
                        "(counter %lu, pid %u)",
                        numSnapshotsAttempted,
                        childPid);
                tracking = numSnapshotsAttempted;
                startProgress = currentProgress;
                startTime = now;
            }
            if (snapshotWatchdogInterval != zero)
                waitUntil = startTime + snapshotWatchdogInterval;
        } else { // no child process
            if (tracking != ~0UL) {
                VERBOSE("Snapshot ended: no longer tracking (counter %lu)",
                        tracking);
                tracking = ~0UL;
            }
        }
        snapshotStarted.wait_until(lockGuard, waitUntil);
    }
}


void
StateMachineBase::takeSnapshot(uint64_t lastIncludedIndex,
                           std::unique_lock<Core::Mutex>& lockGuard)
{
    // Open a snapshot file, then fork a child to write a consistent view of
    // the state machine to the snapshot file while this process continues
    // accepting requests.
    writer = consensus->beginSnapshot(lastIncludedIndex);
    // Flush the outstanding changes to the snapshot now so that they
    // aren't somehow double-flushed later.
    writer->flushToOS();

    ++numSnapshotsAttempted;
    snapshotStarted.notify_all();

    // Create a read-only snapshot for dumping data to backup
    assert(_options.snapshot == nullptr);
    createSnapshotPoint();

    pid_t pid = fork();
    if (pid == -1) { // error
        PANIC("Couldn't fork: %s", strerror(errno));
    } else if (pid == 0) { // child
        Core::Debug::processName += "-child";
        globals.unblockAllSignals();
        usleep(stateMachineChildSleepMs * 1000); // for testing purposes
        if (snapshotBlockPercentage > 0) { // for testing purposes
            if (Core::Random::randomRange(0, 100) < snapshotBlockPercentage) {
                WARNING("Purposely deadlocking child (probability is %lu%%)",
                        snapshotBlockPercentage);
                std::mutex mutex;
                mutex.lock();
                mutex.lock(); // intentional deadlock
            }
        }

        // Format version of snapshot contents is 1.
        uint8_t formatVersion = 1;
        writer->writeRaw(&formatVersion, sizeof(formatVersion));
        // StateMachineBase state comes next
        {
            SnapshotStateMachine::Header header;
            serializeVersionHistory(header);
            serializeSessions(header);
            writer->writeMessage(header);
        }
        // Then the Tree itself (this one is potentially large)
//        tree.dumpSnapshot(*writer);
        takeSnapshotWriteData(lastIncludedIndex, writer.get());

        // Flush the changes to the snapshot file before exiting.
        writer->flushToOS();
        _exit(0);
    } else { // parent
        assert(childPid == 0);
        childPid = pid;
        int status = 0;
        {
            // release the lock while blocking on the child to allow
            // parallelism
            Core::MutexUnlock<Core::Mutex> unlockGuard(lockGuard);
            pid = waitpid(pid, &status, 0);
        }

        snapshotDone();

        childPid = 0;
        if (pid == -1)
            PANIC("Couldn't waitpid: %s", strerror(errno));
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            NOTICE("Child completed writing state machine contents to "
                   "snapshot staging file");
            writer->seekToEnd();
            consensus->snapshotDone(lastIncludedIndex, std::move(writer));
        } else if (exiting &&
                   WIFSIGNALED(status) && WTERMSIG(status) == SIGTERM) {
            writer->discard();
            writer.reset();
            NOTICE("Child exited from SIGTERM since this process is "
                   "exiting");
        } else {
            writer->discard();
            writer.reset();
            ++numSnapshotsFailed;
            ERROR("Snapshot creation failed with status %d. This server will "
                  "try again, but something might be terribly wrong. "
                  "%lu of %lu snapshots have failed in total.",
                  status,
                  numSnapshotsFailed,
                  numSnapshotsAttempted);
        }
        snapshotCompleted.notify_all();
    }
}

void
StateMachineBase::warnUnknownRequest(
        const google::protobuf::Message& request,
        const char* reason) const
{
    ++numUnknownRequests;
    TimePoint now = Clock::now();
    if (lastUnknownRequestMessage + unknownRequestMessageBackoff < now) {
        lastUnknownRequestMessage = now;
        if (numUnknownRequestsSinceLastMessage > 0) {
            WARNING("This version of the state machine (%u) %s "
                    "(and %lu similar warnings "
                    "were suppressed since the last message): %s",
                    getVersion(~0UL),
                    reason,
                    numUnknownRequestsSinceLastMessage,
                    Core::ProtoBuf::dumpString(request).c_str());
        } else {
            WARNING("This version of the state machine (%u) %s: %s",
                    getVersion(~0UL),
                    reason,
                    Core::ProtoBuf::dumpString(request).c_str());
        }
        numUnknownRequestsSinceLastMessage = 0;
    } else {
        ++numUnknownRequestsSinceLastMessage;
    }
}

const std::shared_ptr<RaftConsensus> &StateMachineBase::getConsensus() const {
    return consensus;
}

} // namespace LogCabin::Server
} // namespace LogCabin
