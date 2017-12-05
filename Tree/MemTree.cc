#include <algorithm>

#include "build/Tree/Snapshot.pb.h"
#include "Core/StringUtil.h"
#include "Tree/MemTree.h"

namespace LogCabin {
namespace Tree{
using Core::StringUtil::format;
using namespace Internal;

namespace Internal {

////////// class File //////////

File::File()
    : contents()
, list()
, sset()
, iset()
{
}

void
File::dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const
{
    Snapshot::File file;
    file.set_contents(contents);
    for (auto i = list.begin(); i != list.end(); i++) {
        file.mutable_list()->add_items(*i);
    }
    for (auto i : sset) {
        file.mutable_sset()->add_items(i);
    }
    for (auto i : iset) {
        file.mutable_iset()->add_items(i);
    }
    stream.writeMessage(file);
}

void
File::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
    Snapshot::File node;
    std::string error = stream.readMessage(node);
    if (!error.empty()) {
        PANIC("Couldn't read snapshot: %s", error.c_str());
    }
    contents = node.contents();
    Snapshot::List l = node.list();
    for (auto i = 0; i < l.items_size(); i++) {
        list.push_back(l.items(i));
    }
    for (auto i = 0; i < node.sset().items_size(); i++) {
        sset.insert(node.sset().items(i));
    }
    for (auto i = 0; i < node.iset().items_size(); i++) {
        iset.insert(node.iset().items(i));
    }
}

uint64_t
File::size() const {
    uint64_t size = sizeof(File);

    size += contents.size();
    for (auto i : list) {
        size += i.size();
    }
    for (auto i : sset) {
        size += i.size();
    }
    size += iset.size() * 8;

    return size;
}

////////// class Directory //////////

Directory::Directory()
    : directories()
    , files()
{
}

uint64_t
Directory::size() const {
    uint64_t size = sizeof(Directory);

    for (auto it = directories.begin(); it != directories.end(); ++it) {
        size += it->second.size();
    }
    for (auto it = files.begin(); it != files.end(); ++it) {
        size += it->second.size();
    }
    return size;
}

std::vector<std::string>
Directory::getChildren() const
{
    std::vector<std::string> children;
    for (auto it = directories.begin(); it != directories.end(); ++it)
        children.push_back(it->first + "/");
    for (auto it = files.begin(); it != files.end(); ++it)
        children.push_back(it->first);
    return children;
}

Directory*
Directory::lookupDirectory(const std::string& name)
{
    return const_cast<Directory*>(
        const_cast<const Directory*>(this)->lookupDirectory(name));
}

const Directory*
Directory::lookupDirectory(const std::string& name) const
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    auto it = directories.find(name);
    if (it == directories.end())
        return NULL;
    return &it->second;
}


Directory*
Directory::makeDirectory(const std::string& name)
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    if (lookupFile(name) != NULL)
        return NULL;
    return &directories[name];
}

void
Directory::removeDirectory(const std::string& name)
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    directories.erase(name);
}

File*
Directory::lookupFile(const std::string& name)
{
    return const_cast<File*>(
        const_cast<const Directory*>(this)->lookupFile(name));
}

const File*
Directory::lookupFile(const std::string& name) const
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    auto it = files.find(name);
    if (it == files.end())
        return NULL;
    return &it->second;
}

File*
Directory::makeFile(const std::string& name)
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    if (lookupDirectory(name) != NULL)
        return NULL;
    return &files[name];
}

bool
Directory::removeFile(const std::string& name)
{
    assert(!name.empty());
    assert(!Core::StringUtil::endsWith(name, "/"));
    return (files.erase(name) > 0);
}

void
Directory::dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const
{
    // create protobuf of this dir, listing all children
    Snapshot::Directory dir;
    for (auto it = directories.begin(); it != directories.end(); ++it)
        dir.add_directories(it->first);
    for (auto it = files.begin(); it != files.end(); ++it)
        dir.add_files(it->first);

    for (auto i : sset) {
        dir.mutable_sset()->add_items(i);
    }

    // write dir into stream
    stream.writeMessage(dir);

    // dump children in the same order
    for (auto it = directories.begin(); it != directories.end(); ++it)
        it->second.dumpSnapshot(stream);
    for (auto it = files.begin(); it != files.end(); ++it)
        it->second.dumpSnapshot(stream);
}

void
Directory::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
    Snapshot::Directory dir;
    std::string error = stream.readMessage(dir);
    if (!error.empty()) {
        PANIC("Couldn't read snapshot: %s", error.c_str());
    }
    for (auto it = dir.directories().begin();
         it != dir.directories().end();
         ++it) {
        directories[*it].loadSnapshot(stream);
    }
    for (auto it = dir.files().begin();
         it != dir.files().end();
         ++it) {
        files[*it].loadSnapshot(stream);
    }
}

////////// class Path //////////

Path::Path(const std::string& symbolic)
    : result()
    , symbolic(symbolic)
    , parents()
    , target()
{
    if (!Core::StringUtil::startsWith(symbolic, "/")) {
        result.status = Status::INVALID_ARGUMENT;
        result.error = format("'%s' is not a valid path",
                              symbolic.c_str());
        return;
    }

    // Add /root prefix (see docs for Tree::superRoot)
    parents.push_back("root");

    // Split the path into a list of parent components and a target.
    std::string word;
    for (auto it = symbolic.begin(); it != symbolic.end(); ++it) {
        if (*it == '/') {
            if (!word.empty()) {
                parents.push_back(word);
                word.clear();
            }
        } else {
            word += *it;
        }
    }
    if (!word.empty())
        parents.push_back(word);
    target = parents.back();
    parents.pop_back();
}

std::string
Path::parentsThrough(std::vector<std::string>::const_iterator end) const
{
    auto it = parents.begin();
    ++it; // skip "root"
    ++end; // end was inclusive, now exclusive
    if (it == end)
        return "/";
    std::string ret;
    do {
        ret += "/" + *it;
        ++it;
    } while (it != end);
    return ret;
}

} // LogCabin::Tree::Internal

/*
MemTree::MemTree()
    :superRoot()
{
    superRoot.makeDirectory("root");
}
*/

void 
MemTree::Init(const std::string& path)
{
    // Create the root directory,
    // so that users don't have 
    // to explicitly call makeDirectory("/").
    superRoot.makeDirectory("root");
}

Result
MemTree::normalLookup(const Path& path, Directory** parent)
{
    return normalLookup(path,
                        const_cast<const Directory**>(parent));
}

Result
MemTree::normalLookup(const Path& path, const Directory** parent) const
{
    *parent = NULL;
    Result result;
    const Directory* current = &superRoot;
    for (auto it = path.parents.begin(); it != path.parents.end(); ++it) {
        const Directory* next = current->lookupDirectory(*it);
        if (next == NULL) {
            if (current->lookupFile(*it) == NULL) {
                result.status = Status::LOOKUP_ERROR;
                result.error = format("Parent %s of %s does not exist",
                                      path.parentsThrough(it).c_str(),
                                      path.symbolic.c_str());
            } else {
                result.status = Status::TYPE_ERROR;
                result.error = format("Parent %s of %s is a file",
                                      path.parentsThrough(it).c_str(),
                                      path.symbolic.c_str());
            }
            return result;
        }
        current = next;
    }
    *parent = current;
    return result;
}

Result
MemTree::mkdirLookup(const Path& path, Directory** parent)
{
    *parent = NULL;
    Result result;
    Directory* current = &superRoot;
    for (auto it = path.parents.begin(); it != path.parents.end(); ++it) {
        Directory* next = current->makeDirectory(*it);
        if (next == NULL) {
            result.status = Status::TYPE_ERROR;
            result.error = format("Parent %s of %s is a file",
                                  path.parentsThrough(it).c_str(),
                                  path.symbolic.c_str());
            return result;
        }
        current = next;
    }
    *parent = current;
    return result;
}

void
MemTree::dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const
{
    superRoot.dumpSnapshot(stream);
}

void
MemTree::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
    superRoot = Directory();
    superRoot.loadSnapshot(stream);
}


Result
MemTree::makeDirectory(const std::string& symbolicPath)
{

    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = mkdirLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    if (parent->makeDirectory(path.target) == NULL) {
        result.status = Status::TYPE_ERROR;
        result.error = format("%s already exists but is a file",
                              path.symbolic.c_str());
    }
    return result;
}

Result
MemTree::rpush(const std::string& path, const std::string& contents,int64_t request_time)
{
    Result result;
    return result;
}

Result
MemTree::lpush(const std::string& path, const std::string& contents,int64_t request_time)
{
    Result result;
    return result;
}
Result
MemTree::lpop(const std::string& path, const std::string& contents, int64_t requestTime)
{
    Result result;
    return result;
}
Result
MemTree::lrem(const std::string& path, const std::string& contents, const int32_t count, int64_t requestTime)
{
    Result result;
    return result;
}

Result
MemTree::ltrim(const std::string& path, const std::vector<std::string>& contents, int64_t requestTime)
{
    Result result;
    return result;
}
Result
MemTree::expire(const std::string& path, const int64_t expire, const uint32_t op, int64_t request_time)
{
    Result result;
    return result;
}

Result
MemTree::remove(const std::string& path)
{
    Result result;
    return result;
}
Result
MemTree::lrange(const std::string& path, const std::vector<std::string>& args, std::vector<std::string>& output)
{
    Result result;
    return result;
}

int64_t
MemTree::getKeyExpireTime(const std::string& path)
{
    return -1;
}

Result MemTree::cleanExpiredKeys(const std::string& path)
{
    Result result;
    return result;
}

void
MemTree::cleanUpExpireKeyEvent()
{
}

void
MemTree::startSnapshot(uint64_t lastIncludedIndex)
{
}

Result
MemTree::removeExpireSetting(const std::string& path)
{
    Result result;
    return result;
}

Result
MemTree::scard(const std::string& symbolicPath,
                    std::string& content) const
{
    Result result;
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
    {
        content = "0";
        return result;
    }
    const Directory* parent;
    result = normalLookup(path, &parent);
    if (result.status != Status::OK)
    {
        content = "0";
        result.status = Status::OK;
        return result;
    }

    const File* targetFile = parent->lookupFile(path.target);
    if (targetFile == NULL) {
        content = "0";
        result.status = Status::OK;
        return result;
    }

    content = std::to_string(targetFile->sset.size());
    return result;
}

Result
MemTree::smembers(const std::string& inputSymbolicPath,
                    std::vector<std::string>& children) const
{
    std::string symbolicPath = inputSymbolicPath;
    if(Core::StringUtil::endsWith(symbolicPath, "/"))
    {
        symbolicPath += '/';
    }

    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    const Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;

    children.resize(parent->sset.size());


    long index = 0;
    for (auto i : parent->sset) {
        children[index] = i;
        index++;
    }

    return result;
}

Result
MemTree::listDirectory(const std::string& symbolicPath,
                    std::vector<std::string>& children) const
{
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    const Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    const Directory* targetDir = parent->lookupDirectory(path.target);
    if (targetDir == NULL) {
        if (parent->lookupFile(path.target) == NULL) {
            result.status = Status::LOOKUP_ERROR;
            result.error = format("%s does not exist",
                                  path.symbolic.c_str());
        } else {
            result.status = Status::TYPE_ERROR;
            result.error = format("%s is a file",
                                  path.symbolic.c_str());
        }
        return result;
    }
    children = targetDir->getChildren();
    return result;
}

Result
MemTree::removeDirectory(const std::string& symbolicPath)
{
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status == Status::LOOKUP_ERROR) {
        // no parent, already done
        return Result();
    }
    if (result.status != Status::OK)
        return result;
    Directory* targetDir = parent->lookupDirectory(path.target);
    if (targetDir == NULL) {
        if (parent->lookupFile(path.target)) {
            result.status = Status::TYPE_ERROR;
            result.error = format("%s is a file",
                                  path.symbolic.c_str());
            return result;
        } else {
            // target does not exist, already done
            return result;
        }
    }
    parent->removeDirectory(path.target);
    if (parent == &superRoot) { // removeDirectory("/")
        // If the caller is trying to remove the root directory, we remove the
        // contents but not the directory itself. The easiest way to do this
        // is to drop but then recreate the directory.
        parent->makeDirectory(path.target);
    }
    return result;
}

Result
MemTree::write(const std::string& symbolicPath, const std::string& contents, int64_t requestTime)
{
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    File* targetFile = parent->makeFile(path.target);
    if (targetFile == NULL) {
        result.status = Status::TYPE_ERROR;
        result.error = format("%s is a directory",
                              path.symbolic.c_str());
        return result;
    }

    targetFile->contents = contents;

    return result;
}

Result
MemTree::sadd(const std::string& inputSymbolicPath, const std::string& contents)
{
    std::string symbolicPath = inputSymbolicPath;
    if(!Core::StringUtil::endsWith(symbolicPath, "/"))
    {
        symbolicPath += '/';
    }
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
//    Result result = normalLookup(path, &parent);
    Result result = mkdirLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    parent->sset.insert(contents);

    return result;
}

Result
MemTree::srem(const std::string& inputSymbolicPath, const std::string& contents)
{
    std::string symbolicPath = inputSymbolicPath;
    if(!Core::StringUtil::endsWith(symbolicPath, "/"))
    {
        symbolicPath += '/';
    }
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    if (contents.length() > 0) {
        parent->sset.erase(contents);
    }
    return result;
}

Result
MemTree::read(const std::string& symbolicPath, std::string& contents)
{
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    const Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    const File* targetFile = parent->lookupFile(path.target);
    if (targetFile == NULL) {
        if (parent->lookupDirectory(path.target) != NULL) {
            result.status = Status::TYPE_ERROR;
            result.error = format("%s is a directory",
                                  path.symbolic.c_str());
        } else {
            result.status = Status::LOOKUP_ERROR;
            result.error = format("%s does not exist",
                                  path.symbolic.c_str());
        }
        return result;
    }

    contents = targetFile->contents;
    /*
    for (auto i = targetFile->list.begin(); i != targetFile->list.end(); i++) {
        if (i == targetFile->list.begin() )
            contents = *i;
        else
            contents += "," + *i;
    }

    contents += "\n<";
    for (auto i : targetFile->sset) {
        contents += i + ",";
    }
    // contents.at(contents.length() - 1) = '>';
    contents += "\n<";
    for (auto i : targetFile->iset) {
        std::stringstream ss;
        ss << i;
        contents += ss.str() + ",";
    }
    // contents.at(contents.length() - 1) = ">";
    */
    return result;
}

Result
MemTree::head(const std::string& symbolicPath, std::string& contents) const
{
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    const Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status != Status::OK)
        return result;
    const File* targetFile = parent->lookupFile(path.target);
    if (targetFile == NULL) {
        if (parent->lookupDirectory(path.target) != NULL) {
            result.status = Status::TYPE_ERROR;
            result.error = format("%s is a directory",
                                  path.symbolic.c_str());
        } else {
            result.status = Status::LOOKUP_ERROR;
            result.error = format("%s does not exist",
                                  path.symbolic.c_str());
        }
        return result;
    }
//    contents = targetFile->contents;
//    for (auto i = targetFile->list.begin(); i != targetFile->list.end(); i++) {
//        if (i == targetFile->list.begin() )
//            contents = *i;
//        else
//            contents += "," + *i;
//    }
    if (targetFile->list.empty()) {
        result.status = Status::LIST_EMPTY;
        result.error = format("%s list is empty",
            path.symbolic.c_str());
        return result;
    }
    contents = targetFile->list.front();
    return result;
}

Result
MemTree::removeFile(const std::string& symbolicPath)
{
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
    Directory* parent;
    Result result = normalLookup(path, &parent);
    if (result.status == Status::LOOKUP_ERROR) {
        // no parent, already done
        return Result();
    }
    if (result.status != Status::OK)
        return result;
    if (parent->lookupDirectory(path.target) != NULL) {
        result.status = Status::TYPE_ERROR;
        result.error = format("%s is a directory",
                              path.symbolic.c_str());
        return result;
    }
    parent->removeFile(path.target);
    return result;
}

} // LogCabin::Tree
} // LogCabin
