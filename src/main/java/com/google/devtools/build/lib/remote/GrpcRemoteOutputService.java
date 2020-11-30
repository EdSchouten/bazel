package com.google.devtools.build.lib.remote;

import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.partition;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.OutputSymlink;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.build.lib.actions.Action;
import com.google.devtools.build.lib.actions.ActionInputMap;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.BuildFailedException;
import com.google.devtools.build.lib.actions.cache.MetadataHandler;
import com.google.devtools.build.lib.actions.EnvironmentalExecException;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.actions.FilesetOutputSymlink;
import com.google.devtools.build.lib.actions.LostInputsActionExecutionException;
import com.google.devtools.build.lib.actions.MetadataConsumer;
import com.google.devtools.build.lib.events.EventHandler;
import com.google.devtools.build.lib.remote.RemoteOutputServiceGrpc.RemoteOutputServiceBlockingStub;
import com.google.devtools.build.lib.remote.RemoteOutputServiceGrpc.RemoteOutputServiceFutureStub;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.BatchCreateRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.BatchStatRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.BatchStatResponse;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.CleanRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.FileStatus;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.FinalizeBuildRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.InitialOutputPathContents;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.StartBuildRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.StartBuildResponse;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.StatResponse;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.util.AbruptExitException;
import com.google.devtools.build.lib.vfs.BatchStat;
import com.google.devtools.build.lib.vfs.DelegateFileSystem;
import com.google.devtools.build.lib.vfs.DigestHashFunction;
import com.google.devtools.build.lib.vfs.FileStatusWithDigest;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.ModifiedFileSet;
import com.google.devtools.build.lib.vfs.OutputService;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Root;
import com.google.devtools.build.skyframe.SkyFunction.Environment;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

public class GrpcRemoteOutputService implements OutputService, ActionResultDownloader {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final RemoteOutputServiceFutureStub futureStub;
  private final RemoteOutputServiceBlockingStub blockingStub;
  private final String outputBaseId;
  private final PathFragment outputPathPrefix;
  private final String instanceName;
  private final DigestFunction.Value digestFunction;

  private UUID currentBuildId;
  private PathFragment currentOriginalOutputPath;
  private PathFragment currentRelativeOutputPath;
  private UUID previousBuildId;

  public GrpcRemoteOutputService(ReferenceCountedChannel channel, String outputBaseId, PathFragment outputPathPrefix, String instanceName, DigestFunction.Value digestFunction) {
    this.futureStub = RemoteOutputServiceGrpc.newFutureStub(channel);
    this.blockingStub = RemoteOutputServiceGrpc.newBlockingStub(channel);
    this.outputBaseId = outputBaseId;
    this.outputPathPrefix = outputPathPrefix;
    this.instanceName = instanceName;
    this.digestFunction = digestFunction;
  }


  @Override
  public String getFilesSystemName() {
    return "GrpcRemoteOutputService";
  }

  @Override
  public ModifiedFileSet startBuild(
      Path execRoot, String relativeOutputPath,
      EventHandler eventHandler, UUID buildId, boolean finalizeActions)
      throws BuildFailedException, AbruptExitException, InterruptedException {
    // Notify the remote output service that the build is about to
    // start. The remote output service will return the directory in
    // which it wants us to let the build take place.
    //
    // Make the output service aware of the location of the output path
    // from our perspective and any symbolic links that will point to
    // it. This allows the service to properly resolve symbolic links
    // containing absolute paths as part of BatchStat() calls.
    StartBuildRequest.Builder builder = StartBuildRequest.newBuilder();
    builder.setOutputBaseId(outputBaseId);
    builder.setBuildId(buildId.toString());
    builder.setInstanceName(instanceName);
    builder.setDigestFunction(digestFunction);
    builder.setOutputPathPrefix(outputPathPrefix.toString());
    Path originalOutputPath = execRoot.getRelative(relativeOutputPath);
    builder.putOutputPathAliases(originalOutputPath.toString(), ".");
    StartBuildResponse response = blockingStub.startBuild(builder.build());

    // Replace the output path with a symbolic link pointing to the
    // directory managed by the remote output service.
    PathFragment outputPath = outputPathPrefix.getRelative(response.getOutputPathSuffix());
    try {
      try {
        originalOutputPath.deleteTree();
      } catch (FileNotFoundException e) {}
      originalOutputPath.createSymbolicLink(outputPath);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    currentBuildId = buildId;
    currentOriginalOutputPath = originalOutputPath.asFragment();
    currentRelativeOutputPath = PathFragment.create(relativeOutputPath);

    if (previousBuildId == null || !response.hasInitialOutputPathContents()) {
      // Either Bazel or the remote output service has performed no
      // build before.
      return ModifiedFileSet.EVERYTHING_MODIFIED;
    }
    InitialOutputPathContents initialContents = response.getInitialOutputPathContents();
    if (initialContents.getBuildId() != previousBuildId.toString()) {
      // Bazel and the remote output service disagree on the build ID of
      // the previous build.
      return ModifiedFileSet.EVERYTHING_MODIFIED;
    }
    // Bazel and the remote output service agree on the build ID of the
    // previous build. Return the set of paths that have been modified.
    //
    // TODO: Do these paths need to be relative to the exec root or the
    // output path? Assume the exec root for now.
    return ModifiedFileSet.builder()
        .modifyAll(
            Iterables.transform(
                initialContents.getModifiedPathsList(),
                (p) -> currentRelativeOutputPath.getRelative(p)))
        .build();
  }

  @Override
  public void finalizeBuild(boolean buildSuccessful)
      throws BuildFailedException, AbruptExitException, InterruptedException {
    FinalizeBuildRequest.Builder builder = FinalizeBuildRequest.newBuilder();
    builder.setBuildId(currentBuildId.toString());
    builder.setBuildSuccessful(buildSuccessful);

    blockingStub.finalizeBuild(builder.build());

    previousBuildId = currentBuildId;
    currentBuildId = null;
    currentOriginalOutputPath = null;
    currentRelativeOutputPath = null;
  }

  @Override
  public void finalizeAction(Action action, MetadataHandler metadataHandler)
      throws IOException, EnvironmentalExecException, InterruptedException {
    // TODO: Would this be the right place to call into the remote
    // output service to check whether any I/O errors occurred? If so,
    // we should likely let createActionFileSystem() call into the
    // remote output service to start capturing I/O errors.
  }

  private String fixupExecRootPath(PathFragment path) {
    return path.relativeTo(currentRelativeOutputPath).toString();
  }

  private class GrpcBatchStat implements BatchStat {
    private abstract class DumbFileStatus implements FileStatusWithDigest {
      @Override
      public long getSize() throws IOException {
        throw new RuntimeException("Not implemented");
      }

      @Override
      public long getLastModifiedTime() throws IOException {
        throw new RuntimeException("Not implemented");
      }

      @Override
      public long getLastChangeTime() throws IOException {
        return -1;
      }

      @Override
      public long getNodeId() throws IOException {
        throw new RuntimeException("Not implemented");
      }

      @Override
      public byte[] getDigest() throws IOException {
        throw new RuntimeException("Not implemented");
      }
    }

    private class RegularFileStatus extends DumbFileStatus {
      private final long size;
      private final byte[] digest;

      RegularFileStatus(long size, byte[] digest) {
        this.size = size;
        this.digest = digest;
      }

      @Override
      public boolean isFile() {
        return true;
      }

      @Override
      public boolean isDirectory() {
        return false;
      }

      @Override
      public boolean isSymbolicLink() {
        return false;
      }

      @Override
      public boolean isSpecialFile() {
        return false;
      }

      @Override
      public long getSize() throws IOException {
        return size;
      }

      @Override
      public byte[] getDigest() throws IOException {
        return digest;
      }
    }

    private class SymlinkFileStatus extends DumbFileStatus {
      @Override
      public boolean isFile() {
        return false;
      }

      @Override
      public boolean isDirectory() {
        return false;
      }

      @Override
      public boolean isSymbolicLink() {
        return true;
      }

      @Override
      public boolean isSpecialFile() {
        return false;
      }
    }

    public List<FileStatusWithDigest> batchStat(boolean includeDigest,
                                                boolean includeLinks,
                                                Iterable<PathFragment> paths)
        throws IOException, InterruptedException {
      // TODO: Do we need to partition the input, just like in
      // createSymlinkTree(), or is input already guaranteed to be
      // bounded in size?
      BatchStatRequest.Builder builder = BatchStatRequest.newBuilder();
      builder.setBuildId(currentBuildId.toString());
      builder.setIncludeFileDigest(includeDigest);
      for (PathFragment path : paths) {
        builder.addPaths(fixupExecRootPath(path));
      }

      BatchStatResponse responses = blockingStub.batchStat(builder.build());
      return Lists.newArrayList(
          Iterables.transform(
              responses.getResponsesList(),
              (response) -> {
                if (!response.hasFileStatus()) {
                  // File not found.
                  return null;
                }
                FileStatus fileStatus = response.getFileStatus();
                if (fileStatus.hasFile()) {
                  Digest digest = fileStatus.getFile().getDigest();
                  return new RegularFileStatus(digest.getSizeBytes(), DigestUtil.toBinaryDigest(digest));
                }
                if (fileStatus.hasDirectory()) {
                  throw new RuntimeException("DIRECTORY");
                }
                if (fileStatus.hasSymlink()) {
                  return new SymlinkFileStatus();
                }
                if (fileStatus.hasExternal()) {
                  throw new RuntimeException("EXTERNAL");
                }
                throw new RuntimeException("SOMETHING ELSE");
              }));
    }
  }

  @Override
  public BatchStat getBatchStatter() {
    return new GrpcBatchStat();
  }

  @Override
  public boolean canCreateSymlinkTree() {
    return true;
  }

  @Override
  public void createSymlinkTree(Map<PathFragment, PathFragment> symlinks, PathFragment symlinkTreeRoot)
      throws ExecException, InterruptedException {
    // The provided set of symbolic links may be too large to provide to
    // the remote output service at once. Partition the symbolic links
    // in groups of 1000, so that BatchCreateRequest messages remain
    // small enough.
    UnmodifiableIterator<List<Map.Entry<PathFragment, PathFragment>>> symlinkBatchIterator =
        partition(
            filter(symlinks.entrySet().iterator(), (symlink) -> symlink.getValue() != null),
            1000);
    boolean cleanPathPrefix = true;
    while (symlinkBatchIterator.hasNext()) {
      List<Map.Entry<PathFragment, PathFragment>> symlinksBatch = symlinkBatchIterator.next();
      BatchCreateRequest.Builder builder = BatchCreateRequest.newBuilder();
      builder.setBuildId(currentBuildId.toString());
      builder.setPathPrefix(symlinkTreeRoot.toString());
      builder.setCleanPathPrefix(cleanPathPrefix);
      for (Map.Entry<PathFragment, PathFragment> symlink : symlinksBatch) {
        OutputSymlink.Builder symlinkBuilder = builder.addSymlinksBuilder();
        symlinkBuilder.setPath(symlink.getKey().toString());
        symlinkBuilder.setTarget(symlink.getValue().toString());
      }
      blockingStub.batchCreate(builder.build());
      cleanPathPrefix = false;
    }
  }

  @Override
  public void clean() throws ExecException, InterruptedException {
    CleanRequest.Builder builder = CleanRequest.newBuilder();
    builder.setOutputBaseId(outputBaseId);
    blockingStub.clean(builder.build());
  }

  @Override
  public boolean isRemoteFile(Artifact file) {
    // Call BatchStat() on the file.
    BatchStatRequest.Builder builder = BatchStatRequest.newBuilder();
    builder.setBuildId(currentBuildId.toString());
    builder.setIncludeFileDigest(true);
    builder.setFollowSymlinks(true);
    PathFragment fullPath = file.getPath().asFragment();
    PathFragment relativePath;
    try {
      relativePath = fullPath.relativeTo(currentOriginalOutputPath);
    } catch (IllegalArgumentException e) {
      return false;
    }
    builder.addPaths(relativePath.toString());

    BatchStatResponse responses = blockingStub.batchStat(builder.build());
    StatResponse response = responses.getResponses(0);
    if (!response.hasFileStatus()) {
      throw new RuntimeException(format("Remote output service reported file %s as being nonexistent", fullPath.toString()));
    }
    FileStatus fileStatus = response.getFileStatus();
    if (fileStatus.hasFile()) {
      // A regular file. Return the is_remote property as reported by
      // the remote output service.
      return fileStatus.getFile().getIsRemote();
    }
    if (fileStatus.hasExternal()) {
      // The path resolves to a location outside of the output path,
      // meaning that it is available locally.
      return false;
    }
    throw new RuntimeException(format("Path %s does not correspond to a regular file", fullPath.toString()));
  }

  @Override
  public ActionFileSystemType actionFileSystemType() {
    return ActionFileSystemType.STAGE_REMOTE_FILES;
  }

  private class GrpcFileSystem extends DelegateFileSystem {
    private final Path originalOutputPath;

    public GrpcFileSystem(FileSystem sourceDelegate, PathFragment originalOutputPath) {
      super(sourceDelegate);
      this.originalOutputPath = getPath(originalOutputPath);
    }

    @Override
    protected byte[] getFastDigest(Path path) throws IOException {
      // Don't attempt to compute digests ourselves. Call into the
      // remote output service to request the digest. The service may be
      // able to return the digest from its bookkeeping, as opposed to
      // reading the actual file contents.
      BatchStatRequest.Builder builder = BatchStatRequest.newBuilder();
      builder.setBuildId(currentBuildId.toString());
      builder.setIncludeFileDigest(true);
      builder.setFollowSymlinks(true);
      PathFragment relativePath;
      try {
        relativePath = path.relativeTo(originalOutputPath);
      } catch (IllegalArgumentException e) {
        // Path is outside the output path. Send it to the regular file system.
        return super.getFastDigest(path);
      }
      builder.addPaths(relativePath.toString());

      BatchStatResponse responses = blockingStub.batchStat(builder.build());
      StatResponse response = responses.getResponses(0);
      if (!response.hasFileStatus()) {
        // Output service reports that the file does not exist.
        throw new FileNotFoundException();
      }
      FileStatus fileStatus = response.getFileStatus();
      if (fileStatus.hasFile()) {
        FileStatus.File regularFileStatus = fileStatus.getFile();
        return DigestUtil.toBinaryDigest(regularFileStatus.getDigest());
      }
      if (fileStatus.hasExternal()) {
        // Path is a symbolic link that points to a location outside of
        // the output path.  The output service is unable to give a
        // digest. We must compute it ourselves.
        return super.getFastDigest(
            originalOutputPath.getRelative(fileStatus.getExternal().getNextPath()));
      }
      throw new IOException(format("Remote output service did not return the status of file %s", path));
    }
  }

  @Override
  @Nullable
  public FileSystem createActionFileSystem(
      FileSystem sourceDelegate,
      PathFragment execRootFragment,
      String relativeOutputPath,
      ImmutableList<Root> sourceRoots,
      ActionInputMap inputArtifactData,
      Iterable<Artifact> outputArtifacts,
      boolean trackFailedRemoteReads) {
    return new GrpcFileSystem(sourceDelegate, execRootFragment.getRelative(relativeOutputPath));
  }

  @Override
  public void checkActionFileSystemForLostInputs(FileSystem actionFileSystem, Action action)
      throws LostInputsActionExecutionException {
    // TODO: Do we actually need to implement this? An alternative would
    // be to require BatchStat() to touch the underlying CAS objects,
    // just FindMissingBlobs(). By letting GrpcBatchStat.batchStat()
    // return null for files that have disappeared, Bazel would rebuild
    // them.
    //
    // The downside of this approach is that it won't add any
    // backtracking, but maybe that's acceptable. Plain "Builds without
    // the Bytes" also doesn't provide any facilities for that.
  }

  public ListenableFuture<Void> downloadActionResult(ActionResult actionResult) {
    // Request that all outputs of the action are created. Do make sure
    // to remove the "bazel-out/" prefix from all paths, as that part of
    // the path is not managed by the remote output service.
    BatchCreateRequest.Builder builder = BatchCreateRequest.newBuilder();
    builder.setBuildId(currentBuildId.toString());
    for (OutputFile file : actionResult.getOutputFilesList()) {
      // As there is no guarantee that permissions on files created by
      // the remote output service can be modified, make sure that all
      // downloaded files are marked executable.
      // TODO: Do we only want to do this selectively?
      builder.addFilesBuilder()
          .mergeFrom(file)
          .setPath(fixupExecRootPath(PathFragment.create(file.getPath())))
          .setIsExecutable(true);
    }
    for (OutputDirectory directory : actionResult.getOutputDirectoriesList()) {
      builder.addDirectoriesBuilder()
          .mergeFrom(directory)
          .setPath(fixupExecRootPath(PathFragment.create(directory.getPath())));
    }
    for (OutputSymlink symlink : Iterables.concat(actionResult.getOutputFileSymlinksList(), actionResult.getOutputDirectorySymlinksList())) {
      builder.addSymlinksBuilder()
          .mergeFrom(symlink)
          .setPath(fixupExecRootPath(PathFragment.create(symlink.getPath())));
    }

    return Futures.transform(
        futureStub.batchCreate(builder.build()),
        (result) -> null,
        MoreExecutors.directExecutor());
  }
}
