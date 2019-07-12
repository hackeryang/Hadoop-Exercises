package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;


public class ShowFIleStatusTest {  //可以放在工程的Test文件夹下作为单元测试文件，显示文件状态信息
    private MiniDFSCluster cluster;  //use an in-progress HDFS cluster for testing
    private FileSystem fs;

    @Before
    public void setUp() throws IOException {
        Configuration conf = new Configuration();
        if (System.getProperty("test.build.data") == null) {
            System.setProperty("test.build.data", "/tmp");
        }
        cluster = new MiniDFSCluster.Builder(conf).build();
        fs = cluster.getFileSystem();
        OutputStream out = fs.create(new Path("/dir/file"));  //FileSystem.create()创建输出流
        out.write("content".getBytes("UTF-8"));
        out.close();
    }

    @After
    public void tearDown() throws IOException {
        if (fs != null) {
            fs.close();
        }
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void throwsFileNotFoundForNonExistentFile() throws IOException {
        fs.getFileStatus(new Path("no-such-file"));
    }

    @Test
    public void fileStatusForFile() throws IOException {
        Path file = new Path("/dir/file");
        FileStatus stat = fs.getFileStatus(file);
        Assert.assertThat(stat.getPath().toUri().getPath(), CoreMatchers.is("/dir/file"));
        Assert.assertThat(stat.isDirectory(), CoreMatchers.is(false));
        Assert.assertThat(stat.getLen(), CoreMatchers.is(7L));
        //assertThat(stat.getModificationTime(),is(lessThanOrEqualTo(System.currentTimeMillis())));
        Assert.assertThat(stat.getReplication(), CoreMatchers.is((short) 1));
        Assert.assertThat(stat.getBlockSize(), CoreMatchers.is(128 * 1024 * 1024L));
        Assert.assertThat(stat.getOwner(), CoreMatchers.is(System.getProperty("user.name")));
        Assert.assertThat(stat.getGroup(), CoreMatchers.is("supergroup"));
        Assert.assertThat(stat.getPermission().toString(), CoreMatchers.is("rw-r--r--"));
    }

    @Test
    public void fileStatusForDirectory() throws IOException {
        Path dir = new Path("/dir");
        FileStatus stat = fs.getFileStatus(dir);
        Assert.assertThat(stat.getPath().toUri().getPath(), CoreMatchers.is("/dir"));
        Assert.assertThat(stat.isDirectory(), CoreMatchers.is(true));
        Assert.assertThat(stat.getLen(), CoreMatchers.is(0L));
        //assertThat(stat.getModificationTime(),is(lessThanOrEqualTo(System.currentTimeMillis())));
        Assert.assertThat(stat.getReplication(), CoreMatchers.is((short) 0));
        Assert.assertThat(stat.getBlockSize(), CoreMatchers.is(0L));
        Assert.assertThat(stat.getOwner(), CoreMatchers.is(System.getProperty("user.name")));
        Assert.assertThat(stat.getGroup(), CoreMatchers.is("supergroup"));
        Assert.assertThat(stat.getPermission().toString(), CoreMatchers.is("rwxr-xr-x"));
    }
}
