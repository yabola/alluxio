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

package alluxio.underfs.abfs;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.options.FileLocationOptions;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An {@link UnderFileSystem} uses the Microsoft Azure Blob Storage.
 */
@ThreadSafe
public class AbfsUnderFileSystem extends HdfsUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsUnderFileSystem.class);

  /**
   * Constant for the abfs URI scheme.
   */
  public static final String SCHEME_INSECURE = "abfs://";

  /**
   * Constant for the abfss URI scheme.
   */
  public static final String SCHEME_SECURE = "abfss://";

  public static final Pattern STORAGE_ACCOUNT =
      Pattern.compile("fs\\.azure\\.account\\.key\\.(\\w+)\\.dfs\\.core\\.windows\\.net");

  public static final Pattern AZURE_CHINA_STORAGE_ACCOUNT =
      Pattern.compile("fs\\.azure\\.account\\.key\\.(\\w+)\\.dfs\\.core\\.chinacloudapi\\.cn");

  /**
   * Prepares the configuration for this Abfs as an HDFS configuration.
   *
   * @param conf the configuration for this UFS
   * @return the created configuration
   */
  public static Configuration createConfiguration(UnderFileSystemConfiguration conf) {
    Configuration abfsConf = HdfsUnderFileSystem.createConfiguration(conf);
    for (Map.Entry<String, String> entry : conf.toMap().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      Matcher matcher1 = STORAGE_ACCOUNT.matcher(key);
      Matcher matcher2 = AZURE_CHINA_STORAGE_ACCOUNT.matcher(key);
      if (matcher1.matches() || matcher2.matches()) {
        abfsConf.set(key, value);
      }
    }
    return abfsConf;
  }

  /**
   * Factory method to construct a new Wasb {@link UnderFileSystem}.
   *
   * @param uri  the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return a new Abfs {@link UnderFileSystem} instance
   */
  public static AbfsUnderFileSystem createInstance(AlluxioURI uri,
                                                   UnderFileSystemConfiguration conf) {
    Configuration abfsConf = createConfiguration(conf);
    return new AbfsUnderFileSystem(uri, conf, abfsConf);
  }

  /**
   * Constructs a new Abfs {@link UnderFileSystem}.
   *
   * @param ufsUri   the {@link AlluxioURI} for this UFS
   * @param conf     the configuration for this UFS
   * @param abfsConf the configuration for this Abfs UFS
   */
  public AbfsUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
                             final Configuration abfsConf) {
    super(ufsUri, conf, abfsConf);
  }

  @Override
  public String getUnderFSType() {
    return "alluxio/underfs/abfs";
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    // wasb is an object store, so use the default block size, like other object stores.
    return mUfsConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.debug("getFileLocations is not supported when using AbfsUnderFileSystem.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
    throws IOException {
    LOG.debug("getFileLocations is not supported when using AbfsUnderFileSystem.");
    return null;
  }
}
