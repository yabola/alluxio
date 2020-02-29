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

package org.apache.hadoop.fs.azure;

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Used for list azure files recursive.
 */
public class RecursiveListAzureFiles {
  private static final Logger LOG = LoggerFactory.getLogger(RecursiveListAzureFiles.class);
  static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  private static final long MAX_AZURE_BLOCK_SIZE = 512 * 1024 * 1024L;
  static final String AZURE_TEMP_FOLDER = "_$azuretmpfolder$";
  private static final Pattern TRAILING_PERIOD_PLACEHOLDER_PATTERN =
          Pattern.compile("\\[\\[\\.\\]\\](?=$|/)");

  /**
   * List azure files with flatlist.
   * @param fs azure fs impl
   * @param path path of file/dir
   * @return fileStatus[]
   * @throws IOException
   */
  public static FileStatus[] listAzureFiles(NativeAzureFileSystem fs, String path)
          throws IOException {
    AzureNativeFileSystemStore store = fs.getStore();
    Path absolutePath = fs.makeAbsolute(new Path(path));
    long blockSize = fs.getConf().getLong(AZURE_BLOCK_SIZE_PROPERTY_NAME,
            MAX_AZURE_BLOCK_SIZE);
    String key = fs.pathToKey(absolutePath);
    FileMetadata meta = null;
    Set<FileStatus> status = new TreeSet<>();
    try {
      meta = store.retrieveMetadata(key);
    } catch (IOException ex) {

      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
              && NativeAzureFileSystemHelper
              .isFileNotFoundException((StorageException) innerException)) {

        throw new FileNotFoundException(String.format("%s is not found", path));
      }

      throw ex;
    }
    if (meta != null) {
      if (!meta.isDir()) {
        if (!meta.isDir()) {
          return new FileStatus[]{newFile(meta, absolutePath, blockSize, fs.getUri(),
                  fs.getWorkingDirectory())};
        }
      } else {
        PartialListing listing = store.listAll(key, -1,
                -1, null);
        LOG.info("recursive list azure files {} with prefix {}", listing.getFiles().length,
                listing.getCommonPrefixes());
        for (FileMetadata fileMetadata : listing.getFiles()) {
          Path subpath = keyToPath(fileMetadata.getKey());

          // Test whether the metadata represents a file or directory and
          // add the appropriate metadata object.
          //
          // Note: There was a very old bug here where directories were added
          // to the status set as files flattening out recursive listings
          // using "-lsr" down the file system hierarchy.
          if (fileMetadata.isDir()) {
            // Make sure we hide the temp upload folder
            if (fileMetadata.getKey().equals(AZURE_TEMP_FOLDER)) {
              // Don't expose that.
              continue;
            }
            status.add(newDirectory(fileMetadata, subpath, blockSize, fs.getUri(),
                    fs.getWorkingDirectory()));
          } else {
            status.add(newFile(fileMetadata, subpath, blockSize, fs.getUri(),
                    fs.getWorkingDirectory()));
          }
        }
      }
    } else {
      throw new FileNotFoundException("File" + path + " does not exist.");
    }
    return status.toArray(new FileStatus[0]);
  }

  private static FileMetadata[] listBlobs(AzureNativeFileSystemStore store, String key)
          throws NoSuchFieldException,
          IllegalAccessException, URISyntaxException, StorageException {
    Field field = AzureNativeFileSystemStore.class.getDeclaredField("rootDirectory");
    field.setAccessible(true);
    Object obj = field.get(store);
    System.out.println();
    StorageInterface.CloudBlobDirectoryWrapper blobDirectoryWrapper =
            (StorageInterface.CloudBlobDirectoryWrapper) obj;
    CloudBlobContainer blobContainer = blobDirectoryWrapper.getContainer();
    CloudBlobDirectory blobDirectory = blobContainer.getDirectoryReference(key);
    List<ListBlobItem> list = Lists.newArrayList(blobDirectory.listBlobs(null,
            true, EnumSet.noneOf(BlobListingDetails.class),
            null, null));
    return null;
  }

  private static Path keyToPath(String key) {
    if (key.equals("/")) {
      return new Path("/"); // container
    }
    return new Path("/" + decodeTrailingPeriod(key));
  }

  private static String decodeTrailingPeriod(String toDecode) {
    Matcher matcher = TRAILING_PERIOD_PLACEHOLDER_PATTERN.matcher(toDecode);
    return matcher.replaceAll(".");
  }

  private static FileStatus newFile(FileMetadata meta, Path path, long blockSize, URI uri,
                                    Path working) {
    return new FileStatus(
            meta.getLength(),
            false,
            1,
            blockSize,
            meta.getLastModified(),
            0,
            meta.getPermissionStatus().getPermission(),
            meta.getPermissionStatus().getUserName(),
            meta.getPermissionStatus().getGroupName(),
            path.makeQualified(uri, working));
  }

  private static FileStatus newDirectory(FileMetadata meta, Path path, long blockSize, URI uri,
                                         Path working) {
    return new FileStatus(
            0,
            true,
            1,
            blockSize,
            meta == null ? 0 : meta.getLastModified(),
            0,
            meta == null ? FsPermission.getDefault() : meta.getPermissionStatus().getPermission(),
            meta == null ? "" : meta.getPermissionStatus().getUserName(),
            meta == null ? "" : meta.getPermissionStatus().getGroupName(),
            path.makeQualified(uri, working));
  }
}
