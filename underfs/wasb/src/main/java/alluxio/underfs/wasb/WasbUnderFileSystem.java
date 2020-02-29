/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.wasb;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.util.UnderFileSystemUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azure.RecursiveListAzureFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * An {@link UnderFileSystem} uses the Microsoft Azure Blob Storage.
 */
@ThreadSafe
public class WasbUnderFileSystem extends HdfsUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(WasbUnderFileSystem.class);

  /** Constant for the wasb URI scheme. */
  public static final String SCHEME = "wasb://";

  /**
   * Prepares the configuration for this Wasb as an HDFS configuration.
   *
   * @param conf the configuration for this UFS
   * @return the created configuration
   */
  public static Configuration createConfiguration(UnderFileSystemConfiguration conf) {
    Configuration wasbConf = HdfsUnderFileSystem.createConfiguration(conf);
    for (Map.Entry<String, String> entry : conf.toMap().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (PropertyKey.Template.UNDERFS_AZURE_ACCOUNT_KEY.matches(key)) {
        wasbConf.set(key, value);
      }
    }
    wasbConf.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
    wasbConf.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
    return wasbConf;
  }

  /**
   * Factory method to construct a new Wasb {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return a new Wasb {@link UnderFileSystem} instance
   */
  public static WasbUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {
    Configuration wasbConf = createConfiguration(conf);
    return new WasbUnderFileSystem(uri, conf, wasbConf);
  }

  /**
   * Constructs a new Wasb {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @param wasbConf the configuration for this Wasb UFS
   */
  public WasbUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
      final Configuration wasbConf) {
    super(ufsUri, conf, wasbConf);
  }

  @Override
  public String getUnderFSType() {
    return "wasb";
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    // wasb is an object store, so use the default block size, like other object stores.
    return mUfsConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.debug("getFileLocations is not supported when using WasbUnderFileSystem.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    LOG.debug("getFileLocations is not supported when using WasbUnderFileSystem.");
    return null;
  }

  @Nullable
  @Override
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    if (!options.isRecursive()) {
      return super.listStatus(path);
    }
    FileSystem fs = getFs();
    fs.getConf().set("fs.azure.flatlist.enable", "true");
    if (fs instanceof NativeAzureFileSystem) {
      LOG.info("fs.azure.flatlist.enable is set {}", fs.getConf().get("fs.azure.flatlist.enable"));
      NativeAzureFileSystem azureFileSystem = (NativeAzureFileSystem) fs;
      FileStatus[] fileStatuses = RecursiveListAzureFiles.listAzureFiles(azureFileSystem, path);
      UfsStatus[] rtn = new UfsStatus[fileStatuses.length];
      int i = 0;
      for (FileStatus status : fileStatuses) {
        String rootPath = URI.create(path).getPath();
        if (!rootPath.endsWith("/")) {
          rootPath = rootPath + "/";
        }
        String targetPath = status.getPath().toUri().getPath().substring(rootPath.length());
        UfsStatus retStatus;
        if (!status.isDirectory()) {
          String contentHash = UnderFileSystemUtils
                  .approximateContentHash(status.getLen(), status.getModificationTime());
          retStatus = new UfsFileStatus(targetPath, contentHash,
                  status.getLen(),
                  status.getModificationTime(), status.getOwner(), status.getGroup(),
                  status.getPermission().toShort(), status.getBlockSize());
        } else {
          retStatus = new UfsDirectoryStatus(targetPath, status.getOwner(),
                  status.getGroup(), status.getPermission().toShort(),
                  status.getModificationTime());
        }
        rtn[i++] = retStatus;
      }
      return rtn;
    }
    return super.listStatus(path, options);
  }
}
