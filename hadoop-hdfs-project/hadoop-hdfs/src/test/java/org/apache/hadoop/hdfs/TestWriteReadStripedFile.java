/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Rule;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

@RunWith(Enclosed.class)
public class TestWriteReadStripedFile {
  public static final Log LOG =
      LogFactory.getLog(TestWriteReadStripedFile.class);
  private static final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private static final int cellSize = ecPolicy.getCellSize();
  private static final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private static final short parityBlocks =
      (short) ecPolicy.getNumParityUnits();
  private static final int stripeSize = cellSize * dataBlocks;
  private static final int numDNs = dataBlocks + parityBlocks;
  private static final int stripesPerBlock = 4;
  private static final int blockSize = stripesPerBlock * cellSize;
  private static final int blockGroupSize = blockSize * dataBlocks;

  @Ignore
  public static class StripedFileIOTestBase {
    MiniDFSCluster cluster;
    DistributedFileSystem fs;
    Configuration conf = new HdfsConfiguration();

    @Rule
    public Timeout globalTimeout = new Timeout(300000);

    static {
      GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.ALL);
      GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.ALL);
      GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
      ((Log4JLogger)LogFactory.getLog(BlockPlacementPolicy.class))
          .getLogger().setLevel(Level.ALL);
    }

    @Before
    public void setup() throws IOException {
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
          false);
      conf.set(DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY,
          StripedFileTestUtil.getDefaultECPolicy().getName());
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
      fs = cluster.getFileSystem();
      fs.mkdirs(new Path("/ec"));
      cluster.getFileSystem().getClient().setErasureCodingPolicy("/ec",
          StripedFileTestUtil.getDefaultECPolicy().getName());
    }

    @After
    public void tearDown() throws IOException {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestReadStripedFiles extends StripedFileIOTestBase {
    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[]{"/ec/EmptyFile", 0})
          .add(new Object[]{"/ec/SmallerThanOneCell", 1})
          .add(new Object[]{"/ec/SmallerThanOneCell2", cellSize - 1})
          .add(new Object[]{"/ec/EqualsWithOneCell", cellSize})
          .add(new Object[]{"/ec/SmallerThanOneStripe", cellSize + 123})
          .add(new Object[]{"/ec/SmallerThanOneStripe2", stripeSize - 1})
          .add(new Object[]{"/ec/EqualsWithOneStripe", stripeSize})
          .add(new Object[]{"/ec/MoreThanOneStripe", stripeSize + 123})
          .add(new Object[]{"/ec/MoreThanOneStripe2", stripeSize * 2 + 123})
          .add(new Object[]{"/ec/LessThanFullBlockGroup",
              stripeSize * (stripesPerBlock - 1) + cellSize})
          .add(new Object[]{"/ec/FullBlockGroup", blockSize * dataBlocks})
          .add(new Object[]{"/ec/MoreThanABlockGroup",
              blockSize * dataBlocks + 123})
          .add(new Object[]{"/ec/MoreThanABlockGroup2",
              blockSize * dataBlocks + cellSize + 123})
          .add(new Object[]{"/ec/MoreThanABlockGroup3",
              blockSize * dataBlocks * 3 + stripeSize + cellSize + 123})
          .build();
    }

    @Parameterized.Parameter(0)
    public String filePath;

    @Parameterized.Parameter(1)
    public int fileLength;

    @Test
    public void testReadStripedFile() throws Exception {
      testOneFileUsingDFSStripedInputStream(filePath, fileLength);
      testOneFileUsingDFSStripedInputStream(filePath + "_DNFailure",
          fileLength, true);
    }

    private void testOneFileUsingDFSStripedInputStream(String src, int fileLength)
        throws Exception {
      testOneFileUsingDFSStripedInputStream(src, fileLength, false);
    }

    private void testOneFileUsingDFSStripedInputStream(String src,
        int fileLength, boolean withDataNodeFailure) throws Exception {
      final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
      Path srcPath = new Path(src);
      DFSTestUtil.writeFile(fs, srcPath, new String(expected));
      StripedFileTestUtil.waitBlockGroupsReported(fs, src);

      StripedFileTestUtil.verifyLength(fs, srcPath, fileLength);

      if (withDataNodeFailure) {
        // TODO: StripedFileTestUtil.random.nextInt(dataBlocks);
        int dnIndex = 1;
        LOG.info("stop DataNode " + dnIndex);
        stopDataNode(srcPath, dnIndex);
      }

      byte[] smallBuf = new byte[1024];
      byte[] largeBuf = new byte[fileLength + 100];
      StripedFileTestUtil.verifyPread(
          fs, srcPath, fileLength, expected, largeBuf);

      StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
          largeBuf);
      StripedFileTestUtil.verifySeek(fs, srcPath, fileLength, ecPolicy,
          blockGroupSize);
      StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
          ByteBuffer.allocate(fileLength + 100));
      StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
          smallBuf);
      StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
          ByteBuffer.allocate(1024));
    }

    private void stopDataNode(Path path, int failedDNIdx)
        throws IOException {
      BlockLocation[] locs = fs.getFileBlockLocations(path, 0, cellSize);
      if (locs != null && locs.length > 0) {
        String name = (locs[0].getNames())[failedDNIdx];
        for (DataNode dn : cluster.getDataNodes()) {
          int port = dn.getXferPort();
          if (name.contains(Integer.toString(port))) {
            dn.shutdown();
            break;
          }
        }
      }
    }
  }


  @RunWith(JUnit4.class)
  public static class BasicStripedFileIOTest extends StripedFileIOTestBase {
    @Test
    public void testWriteReadUsingWebHdfs() throws Exception {
      int fileLength = blockSize * dataBlocks + cellSize + 123;

      final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
      FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      Path srcPath = new Path("/testWriteReadUsingWebHdfs");
      DFSTestUtil.writeFile(fs, srcPath, new String(expected));

      StripedFileTestUtil.verifyLength(fs, srcPath, fileLength);

      byte[] smallBuf = new byte[1024];
      byte[] largeBuf = new byte[fileLength + 100];
      // TODO: HDFS-8797
      //StripedFileTestUtil.verifyPread(fs, srcPath, fileLength, expected, largeBuf);

      StripedFileTestUtil.verifyStatefulRead(
          fs, srcPath, fileLength, expected, largeBuf);
      StripedFileTestUtil.verifySeek(
          fs, srcPath, fileLength, ecPolicy, blockGroupSize);
      StripedFileTestUtil.verifyStatefulRead(
          fs, srcPath, fileLength, expected, smallBuf);
      // webhdfs doesn't support bytebuffer read
    }

    @Test
    public void testConcat() throws Exception {
      final byte[] data =
          StripedFileTestUtil.generateBytes(blockSize * dataBlocks * 10 + 234);
      int totalLength = 0;

      Random r = new Random();
      Path target = new Path("/ec/testConcat_target");
      DFSTestUtil.writeFile(fs, target, Arrays.copyOfRange(data, 0, 123));
      totalLength += 123;

      int numFiles = 5;
      Path[] srcs = new Path[numFiles];
      for (int i = 0; i < numFiles; i++) {
        srcs[i] = new Path("/ec/testConcat_src_file_" + i);
        int srcLength = r.nextInt(blockSize * dataBlocks * 2) + 1;
        DFSTestUtil.writeFile(fs, srcs[i],
            Arrays.copyOfRange(data, totalLength, totalLength + srcLength));
        totalLength += srcLength;
      }

      fs.concat(target, srcs);
      StripedFileTestUtil.verifyStatefulRead(fs, target, totalLength,
          Arrays.copyOfRange(data, 0, totalLength), new byte[1024]);
    }

    @Test
    public void testConcatWithDifferentECPolicy() throws Exception {
      final byte[] data =
          StripedFileTestUtil.generateBytes(blockSize * dataBlocks);
      Path nonECFile = new Path("/non_ec_file");
      DFSTestUtil.writeFile(fs, nonECFile, data);
      Path target = new Path("/ec/non_ec_file");
      fs.rename(nonECFile, target);

      int numFiles = 2;
      Path[] srcs = new Path[numFiles];
      for (int i = 0; i < numFiles; i++) {
        srcs[i] = new Path("/ec/testConcat_src_file_"+i);
        DFSTestUtil.writeFile(fs, srcs[i], data);
      }
      try {
        fs.concat(target, srcs);
        Assert.fail("non-ec file shouldn't concat with ec file");
      } catch (RemoteException e){
        Assert.assertTrue(e.getMessage()
            .contains("have different erasure coding policy"));
      }
    }
  }
}
