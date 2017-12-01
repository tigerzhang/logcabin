#pragma once

#include "TreeStorageLayer.h"

namespace LogCabin {
namespace Tree{
namespace Internal {

/**
 * A leaf object in the Tree; stores an opaque blob of data.
 */
class File {
  public:
    /// Default constructor.
    File();
    /**
     * Write the file to the stream.
     */
    void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const;
    /**
     * Load the file from the stream.
     */
    void loadSnapshot(Core::ProtoBuf::InputStream& stream);
    /**
     * Opaque data stored in the File.
     */
    std::string contents;

    std::list<std::string> list;
    std::set<std::string> sset;
    std::set<uint64_t> iset;

    uint64_t size() const;
};

/**
 * An interior object in the Tree; stores other Directories and Files.
 * Pointers returned by this class are valid until the File or Directory they
 * refer to is removed.
 */
class Directory {
  public:
    /// Default constructor.
    Directory();

    /**
     * List the contents of the directory.
     * \return
     *      The names of the directories and files that this directory
     *      immediately contains. The names of directories in this listing will
     *      have a trailing slash. The order is first directories (sorted
     *      lexicographically), then files (sorted lexicographically).
     */
    std::vector<std::string> getChildren() const;

    uint64_t size() const;

    /**
     * Find the child directory by the given name.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      The directory by the given name, or
     *      NULL if it is not found or a file exists by that name.
     */
    Directory* lookupDirectory(const std::string& name);
    /**
     * Find the child directory by the given name (const version).
     * \copydetails lookupDirectory
     */
    const Directory* lookupDirectory(const std::string& name) const;
    /**
     * Find the child directory by the given name, or create it if it doesn't
     * exist.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      The directory by the given name, or
     *      NULL if a file exists by that name.
     */
    Directory* makeDirectory(const std::string& name);
    /**
     * Remove the child directory by the given name, if any. This will remove
     * all the contents of the directory as well.
     * \param name
     *      Must not contain a trailing slash.
     */
    void removeDirectory(const std::string& name);

    /**
     * Find the child file by the given name.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      The file by the given name, or
     *      NULL if it is not found or a directory exists by that name.
     */
    File* lookupFile(const std::string& name);
    /**
     * Find the child file by the given name (const version).
     * \copydetails lookupFile
     */
    const File* lookupFile(const std::string& name) const;
    /**
     * Find the child file by the given name, or create it if it doesn't exist.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      The file by the given name, or
     *      NULL if a directory exists by that name.
     */
    File* makeFile(const std::string& name);
    /**
     * Remove the child file by the given name, if any.
     * \param name
     *      Must not contain a trailing slash.
     * \return
     *      True if child file removed, false if no such file existed. This is
     *      mostly useful for counting statistics.
     */
    bool removeFile(const std::string& name);

    /**
     * Write the directory and its children to the stream.
     */
    void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const;
    /**
     * Load the directory and its children from the stream.
     */
    void loadSnapshot(Core::ProtoBuf::InputStream& stream);

  private:
    /**
     * Map from names of child directories (without trailing slashes) to the
     * Directory objects.
     */
    std::map<std::string, Directory> directories;
    /**
     * Map from names of child files to the File objects.
     */
    std::map<std::string, File> files;
};

/**
 * This is used by Tree to parse symbolic paths into their components.
 */
class Path {
  public:
    /**
     * Constructor.
     * \param symbolic
     *      A path delimited by slashes. This must begin with a slash.
     *      (It should not include "/root" to arrive at the root directory.)
     * \warning
     *      The caller must check "result" to see if the path was parsed
     *      successfully.
     */
    explicit Path(const std::string& symbolic);

    /**
     * Used to generate error messages during path lookup.
     * \param end
     *      The last component of 'parents' to include in the returned string;
     *      this is typically the component that caused an error in path
     *      traversal.
     * \return
     *      The prefix of 'parents' up to and including the given end position.
     *      This is returned as a slash-delimited string not including "/root".
     */
    std::string
    parentsThrough(std::vector<std::string>::const_iterator end) const;

  public:
    /**
     * Status and error message from the constructor. Possible errors are:
     * - INVALID_ARGUMENT if path is malformed.
     */
    Result result;

    /**
     * The exact argument given to the constructor.
     */
    std::string symbolic;
    /**
     * The directories needed to traverse to get to the target.
     * This usually begins with "root" to get from the super root to the root
     * directory, then includes the components of the symbolic path up to but
     * not including the target. If the symbolic path is "/", this will be
     * empty.
     */
    std::vector<std::string> parents;
    /**
     * The final component of the path.
     * This is usually at the end of the symbolic path. If the symbolic path is
     * "/", this will be "root", used to get from the super root to the root
     * directory.
     */
    std::string target;
};

} // LogCabin::Tree::Internal

class MemTree: public TreeStorageLayer
{
private:
    /**
     * Resolve the final next-to-last component of the given path (the target's
     * parent).
     * \param[in] path
     *      The path whose parent directory to find.
     * \param[out] parent
     *      Upon successful return, points to the target's parent directory.
     * \return
     *      Status and error message. Possible errors are:
     *       - LOOKUP_ERROR if a parent of path does not exist.
     *       - TYPE_ERROR if a parent of path is a file.
     */
    Result
    normalLookup(const Internal::Path& path,
                 Internal::Directory** parent);
    /**
     * Resolve the final next-to-last component of the given path (the target's
     * parent) -- const version.
     * \copydetails normalLookup
     */
    Result
    normalLookup(const Internal::Path& path,
                 const Internal::Directory** parent) const;

    /**
     * Like normalLookup but creates parent directories as necessary.
     * \param[in] path
     *      The path whose parent directory to find.
     * \param[out] parent
     *      Upon successful return, points to the target's parent directory.
     * \return
     *      Status and error message. Possible errors are:
     *       - TYPE_ERROR if a parent of path is a file.
     */
    Result
    mkdirLookup(const Internal::Path& path, Internal::Directory** parent);
    /**
     * This directory contains the root directory. The super root has a single
     * child directory named "root", and the rest of the tree lies below
     * "root". This is just an implementation detail; this class prepends
     * "/root" to every path provided by the caller.
     *
     * This removes a lot of special-case branches because every operation now
     * has a name of a target within a parent directory -- even those operating
     * on the root directory.
     */
    Internal::Directory superRoot;
public:
    MemTree():superRoot(){};
    virtual void Init(const std::string& path) ;
    virtual void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const ;
    virtual void loadSnapshot(Core::ProtoBuf::InputStream& stream) ;
    virtual Result
    makeDirectory(const std::string& symbolicPath) ;

    virtual Result
    listDirectory(const std::string& symbolicPath, std::vector<std::string>& children) const ;

    virtual Result
    smembers(const std::string& symbolicPath, std::vector<std::string>& children) const ;

    virtual Result
    removeDirectory(const std::string& symbolicPath) ;

    virtual Result
    write(const std::string& path, const std::string& contents,int64_t requestTime) ;
    
    virtual Result
    sadd(const std::string& path, const std::string& contents) ;

    virtual Result
    srem(const std::string& path, const std::string& contents) ;

    virtual Result
    rpush(const std::string& path, const std::string& contents,int64_t request_time) ;

    virtual Result
    lpush(const std::string& path, const std::string& contents,int64_t request_time) ;

    virtual Result
    lpop(const std::string& path, std::string& contents, int64_t requestTime) ;

    virtual Result
    lrem(const std::string& path, const std::string& contents, const int32_t count, int64_t requestTime) ;

    virtual Result
    ltrim(const std::string& path, const std::vector<std::string>& contents, int64_t requestTime) ;

    virtual Result
    expire(const std::string& path, const int64_t expire, const uint32_t op, int64_t request_time) ;

    virtual Result
    read(const std::string& path, std::string& contents) ;

    virtual Result
    remove(const std::string& path) ;

    virtual Result
    lrange(const std::string& path, const std::vector<std::string>& args, std::vector<std::string>& output) ;

    virtual Result
    head(const std::string& path, std::string& contents) const ;

    virtual int64_t getKeyExpireTime(const std::string& path) ;

    virtual void startSnapshot(uint64_t lastIncludedIndex) ;

    virtual Result removeExpireSetting(const std::string& path) ;

    virtual void cleanUpExpireKeyEvent() ;
     
    virtual Result removeFile(const std::string& symbolicPath) ;
    virtual Result cleanExpiredKeys(const std::string& path) ;

    virtual ~MemTree(){};
};
} //namespace Tree
} //namespace LogCabin
