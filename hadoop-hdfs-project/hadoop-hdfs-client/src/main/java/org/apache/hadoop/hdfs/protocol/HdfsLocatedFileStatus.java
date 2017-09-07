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
package org.apache.hadoop.hdfs.protocol;

import java.net.URI;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;

/**
 * Interface that represents the over the wire information
 * including block locations for a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HdfsLocatedFileStatus extends HdfsFileStatus {

  private static final long serialVersionUID = 0x23c73328;

  /**
   * Left transient, because {@link #makeQualifiedLocated(URI,Path)}
   * is the user-facing type.
   */
  private transient LocatedBlocks locations;

  /**
   * Constructor
   *
   * @param length size
   * @param isdir if this is directory
   * @param block_replication the file's replication factor
   * @param blocksize the file's block size
   * @param modification_time most recent modification time
   * @param access_time most recent access time
   * @param permission permission
   * @param owner owner
   * @param group group
   * @param symlink symbolic link
   * @param path local path name in java UTF8 format
   * @param fileId the file id
   * @param locations block locations
   * @param feInfo file encryption info
   */
  public HdfsLocatedFileStatus(long length, boolean isdir,
      int block_replication, long blocksize, long modification_time,
      long access_time, FsPermission permission, EnumSet<Flags> flags,
      String owner, String group, byte[] symlink, byte[] path, long fileId,
      LocatedBlocks locations, int childrenNum, FileEncryptionInfo feInfo,
      byte storagePolicy, ErasureCodingPolicy ecPolicy) {
    super(length, isdir, block_replication, blocksize, modification_time,
        access_time, permission, flags, owner, group, symlink, path, fileId,
        childrenNum, feInfo, storagePolicy, ecPolicy);
    this.locations = locations;
  }

  public LocatedBlocks getBlockLocations() {
    return locations;
  }

  /**
   * This function is used to transform the underlying HDFS LocatedBlocks to
   * BlockLocations.
   *
   * If this file is three-replicated, the returned array are just normal
   * BlockLocations like:
   * <pre>
   * BlockLocation(offset: 0, length: BLOCK_SIZE,
   *   hosts: {"host1:9866", "host2:9866, host3:9866"})
   * BlockLocation(offset: BLOCK_SIZE, length: BLOCK_SIZE,
   *   hosts: {"host2:9866", "host3:9866, host4:9866"})
   * </pre>
   *
   * And if a file is erasure-coded, the returned BlockLocation are all actual
   * data blocks, not logical data groups. Each data block will only have one
   * hostname which holds it. Data blocks that belong to the same block group
   * have the same offset of the file and different sizes according to the
   * actual size of striped data stored on this block.
   *
   * Suppose we have a RS_6_3 coded file (6 data units and 3 parity units).
   * 1. If the file size is less than one cell size, then there will be only
   * one BlockLocation returned.
   * 2. If the file size is less than one stripe size, say 3 * CELL_SIZE, then
   * there will be 3 BlockLocations returned.
   * 3. If the file size is less than one group size but greater than one
   * stripe size, then there will be 6 BlockLocations returned.
   * 4. If the file size is greater than one group size, then we will have at
   * least 6 BlockLocation, and the remaining part can be calculated by
   * above-mentioned rules.
   *
   * Here is a simple example of a file with 3 times of CELL_SIZE length.
   * <pre>
   * BlockLocation(offset: 0, length: CELL_SIZE, hosts: {"host1:9866"})
   * BlockLocation(offset: 0, length: CELL_SIZE, hosts: {"host2:9866"})
   * BlockLocation(offset: 0, length: CELL_SIZE, hosts: {"host3:9866"})
   * </pre>
   *
   * Please refer to
   * {@link org.apache.hadoop.hdfs.TestDistributedFileSystemWithECFile} for
   * more detailed cases.
   */
  public final LocatedFileStatus makeQualifiedLocated(URI defaultUri,
      Path path) {
    makeQualified(defaultUri, path);
    if (isErasureCoded()) {
      return new LocatedFileStatus(this,
          DFSUtilClient.getErasureCodedDataBlocks(getBlockLocations()));
    } else {
      return new LocatedFileStatus(this,
          DFSUtilClient.locatedBlocks2Locations(getBlockLocations()));
    }
  }

  @Override
  public boolean equals(Object o) {
    // satisfy findbugs
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    // satisfy findbugs
    return super.hashCode();
  }
}
