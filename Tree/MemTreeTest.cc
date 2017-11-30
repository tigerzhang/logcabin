#include <gtest/gtest.h>

#include "Tree/MemTree.h"
#include "Storage/FilesystemUtil.h"
#include "Storage/Layout.h"
#include "Storage/SnapshotFile.h"
namespace LogCabin {
namespace Tree {
namespace {

using namespace Internal; // NOLINT
TEST(TreeFileTest, dumpSnapshot)
{
    Storage::Layout layout;
    layout.initTemporary();
    {
        Storage::SnapshotFile::Writer writer(layout);
        File f;
        f.contents = "hello, world!";
        f.dumpSnapshot(writer);
        writer.save();
    }
    {
        Storage::SnapshotFile::Reader reader(layout);
        File f;
        f.loadSnapshot(reader);
        EXPECT_EQ("hello, world!", f.contents);
    }
}
TEST(TreeDirectoryTest, getChildren)
{
    Directory d;
    EXPECT_EQ((std::vector<std::string> {
               }), d.getChildren());
    d.makeFile("d");
    d.makeDirectory("c");
    d.makeFile("b");
    d.makeDirectory("a");
    EXPECT_EQ((std::vector<std::string> {
                "a/", "c/", "b", "d",
               }), d.getChildren());
}

TEST(TreeDirectoryTest, lookupDirectory)
{
    Directory d;
    EXPECT_TRUE(NULL == d.lookupDirectory("foo"));
    d.makeFile("foo");
    EXPECT_TRUE(NULL == d.lookupDirectory("foo"));
    d.makeDirectory("bar");
    Directory* d2 = d.lookupDirectory("bar");
    ASSERT_TRUE(d2 != NULL);
    EXPECT_EQ((std::vector<std::string> {
               }), d2->getChildren());
    EXPECT_EQ(d2, d.lookupDirectory("bar"));
}

TEST(TreeDirectoryTest, lookupDirectory_const)
{
    Directory d;
    const Directory& constd = d;
    EXPECT_TRUE(NULL == constd.lookupDirectory("foo"));
    d.makeFile("foo");
    EXPECT_TRUE(NULL == constd.lookupDirectory("foo"));
    d.makeDirectory("bar");
    const Directory* d2 = constd.lookupDirectory("bar");
    ASSERT_TRUE(d2 != NULL);
    EXPECT_EQ((std::vector<std::string> {
               }), d2->getChildren());
    EXPECT_EQ(d2, constd.lookupDirectory("bar"));
}

TEST(TreeDirectoryTest, makeDirectory)
{
    Directory d;
    d.makeFile("foo");
    EXPECT_TRUE(NULL == d.makeDirectory("foo"));
    Directory* d2 = d.makeDirectory("bar");
    ASSERT_TRUE(d2 != NULL);
    EXPECT_EQ((std::vector<std::string> {
               }), d2->getChildren());
    EXPECT_EQ(d2, d.makeDirectory("bar"));
}

TEST(TreeDirectoryTest, removeDirectory)
{
    Directory d;
    d.removeDirectory("foo");
    d.makeDirectory("bar")->makeDirectory("baz");
    d.removeDirectory("bar");
    EXPECT_EQ((std::vector<std::string> {
               }), d.getChildren());
}

TEST(TreeDirectoryTest, lookupFile)
{
    Directory d;
    EXPECT_TRUE(NULL == d.lookupFile("foo"));
    d.makeDirectory("foo");
    EXPECT_TRUE(NULL == d.lookupFile("foo"));
    d.makeFile("bar");
    File* f = d.lookupFile("bar");
    ASSERT_TRUE(f != NULL);
    EXPECT_EQ("", f->contents);
    EXPECT_EQ(f, d.lookupFile("bar"));
}

TEST(TreeDirectoryTest, lookupFile_const)
{
    Directory d;
    const Directory& constd = d;
    EXPECT_TRUE(NULL == constd.lookupFile("foo"));
    d.makeDirectory("foo");
    EXPECT_TRUE(NULL == constd.lookupFile("foo"));
    d.makeFile("bar");
    const File* f = constd.lookupFile("bar");
    ASSERT_TRUE(f != NULL);
    EXPECT_EQ("", f->contents);
    EXPECT_EQ(f, constd.lookupFile("bar"));
}

TEST(TreeDirectoryTest, makeFile)
{
    Directory d;
    d.makeDirectory("foo");
    EXPECT_TRUE(NULL == d.makeFile("foo"));
    File* f = d.makeFile("bar");
    ASSERT_TRUE(f != NULL);
    EXPECT_EQ("", f->contents);
    EXPECT_EQ(f, d.makeFile("bar"));
}

TEST(TreeDirectoryTest, removeFile)
{
    Directory d;
    d.removeFile("foo");
    d.makeFile("bar");
    d.removeFile("bar");
    EXPECT_EQ((std::vector<std::string> {
               }), d.getChildren());
}

TEST(TreePathTest, constructor)
{
    Path p1("");
    EXPECT_EQ(Status::INVALID_ARGUMENT, p1.result.status);

    Path p2("/");
    EXPECT_OK(p2.result);
    EXPECT_EQ("/", p2.symbolic);
    EXPECT_EQ((std::vector<std::string> {
               }), p2.parents);
    EXPECT_EQ("root", p2.target);

    Path p3("/foo");
    EXPECT_OK(p3.result);
    EXPECT_EQ("/foo", p3.symbolic);
    EXPECT_EQ((std::vector<std::string> {
                   "root",
               }), p3.parents);
    EXPECT_EQ("foo", p3.target);

    Path p4("/foo/bar/");
    EXPECT_OK(p4.result);
    EXPECT_EQ("/foo/bar/", p4.symbolic);
    EXPECT_EQ((std::vector<std::string> {
                   "root", "foo",
               }), p4.parents);
    EXPECT_EQ("bar", p4.target);
}

TEST(TreePathTest, parentsThrough)
{
    Path path("/a/b/c");
    auto it = path.parents.begin(); // root
    EXPECT_EQ("/", path.parentsThrough(it));
    ++it; // a
    EXPECT_EQ("/a", path.parentsThrough(it));
    ++it; // b
    EXPECT_EQ("/a/b", path.parentsThrough(it));
    ++it; // c
    EXPECT_EQ("/a/b/c", path.parentsThrough(it));
}

}

} //LogCabin::Tree

}//LogCabin
