/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.zookeeper.server.admin;

import static org.apache.zookeeper.server.persistence.FileSnap.SNAPSHOT_FILE_PREFIX;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.Environment.Entry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Version;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.persistence.SnapshotInfo;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.server.quorum.Follower;
import org.apache.zookeeper.server.quorum.FollowerZooKeeperServer;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.MultipleAddresses;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumZooKeeperServer;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.RateLimiter;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class containing static methods for registering and running Commands, as well
 * as default Command definitions.
 *
 * @see Command
 * @see JettyAdminServer
 */
public class Commands {

    static final Logger LOG = LoggerFactory.getLogger(Commands.class);
    // VisibleForTesting
    static final String ADMIN_RATE_LIMITER_INTERVAL = "zookeeper.admin.rateLimiterIntervalInMS";
    private static final long rateLimiterInterval =
            Integer.parseInt(System.getProperty(ADMIN_RATE_LIMITER_INTERVAL, "300000"));
    // VisibleForTesting
    static final String AUTH_INFO_SEPARATOR = " ";
    // VisibleForTesting
    static final String ROOT_PATH = "/";

    /**
     * Maps command names to Command instances.
     */
    private static Map<String, Command> commands = new HashMap<>();
    private static Set<String> primaryNames = new HashSet<>();

    /**
     * Registers the given command. Registered commands can be run by passing
     * any of their names to runCommand.
     */
    public static void registerCommand(Command command) {
        for (String name : command.getNames()) {
            Command prev = commands.put(name, command);
            if (prev != null) {
                LOG.warn("Re-registering command {} (primary name = {})", name, command.getPrimaryName());
            }
        }
        primaryNames.add(command.getPrimaryName());
    }

    /**
     * Run the registered command with name cmdName. Commands should not produce
     * any exceptions; any (anticipated) errors should be reported in the
     * "error" entry of the returned map. Likewise, if no command with the given
     * name is registered, this will be noted in the "error" entry.
     *
     * @param cmdName
     * @param zkServer
     * @param kwargs   String-valued keyword arguments to the command from HTTP GET request
     *                 (may be null if command requires no additional arguments)
     * @param authInfo auth info for auth check
     *                 (null if command requires no auth check)
     * @param request  HTTP request
     * @return Map representing response to command containing at minimum:
     * - "command" key containing the command's primary name
     * - "error" key containing a String error message or null if no error
     */
    public static CommandResponse runGetCommand(
            String cmdName,
            ZooKeeperServer zkServer,
            Map<String, String> kwargs,
            String authInfo,
            HttpServletRequest request) {
        return runCommand(cmdName, zkServer, kwargs, null, authInfo, request, true);
    }

    /**
     * Run the registered command with name cmdName. Commands should not produce
     * any exceptions; any (anticipated) errors should be reported in the
     * "error" entry of the returned map. Likewise, if no command with the given
     * name is registered, this will be noted in the "error" entry.
     *
     * @param cmdName
     * @param zkServer
     * @param inputStream InputStream from HTTP POST request
     * @return Map representing response to command containing at minimum:
     * - "command" key containing the command's primary name
     * - "error" key containing a String error message or null if no error
     */
    public static CommandResponse runPostCommand(
            String cmdName,
            ZooKeeperServer zkServer,
            InputStream inputStream,
            String authInfo,
            HttpServletRequest request) {
        return runCommand(cmdName, zkServer, null, inputStream, authInfo, request, false);
    }

    private static CommandResponse runCommand(
            String cmdName,
            ZooKeeperServer zkServer,
            Map<String, String> kwargs,
            InputStream inputStream,
            String authInfo,
            HttpServletRequest request,
            boolean isGet) {
        Command command = getCommand(cmdName);
        if (command == null) {
            // set the status code to 200 to keep the current behavior of existing commands
            LOG.warn("Unknown command");
            return new CommandResponse(cmdName, "Unknown command: " + cmdName, HttpServletResponse.SC_OK);
        }
        if (command.isServerRequired() && (zkServer == null || !zkServer.isRunning())) {
            // set the status code to 200 to keep the current behavior of existing commands
            LOG.warn("This ZooKeeper instance is not currently serving requests for command");
            return new CommandResponse(cmdName, "This ZooKeeper instance is not currently serving requests",
                    HttpServletResponse.SC_OK);
        }

        final AuthRequest authRequest = command.getAuthRequest();
        if (authRequest != null) {
            if (authInfo == null) {
                LOG.warn("Auth info is missing for command");
                return new CommandResponse(cmdName, "Auth info is missing for the command",
                        HttpServletResponse.SC_UNAUTHORIZED);
            }
            try {
                final List<Id> ids = handleAuthentication(request, authInfo);
                handleAuthorization(zkServer, ids, authRequest.getPermission(), authRequest.getPath());
            } catch (final KeeperException.AuthFailedException e) {
                return new CommandResponse(cmdName, "Not authenticated", HttpServletResponse.SC_UNAUTHORIZED);
            } catch (final KeeperException.NoAuthException e) {
                return new CommandResponse(cmdName, "Not authorized", HttpServletResponse.SC_FORBIDDEN);
            } catch (final Exception e) {
                LOG.warn("Error occurred during auth for command", e);
                return new CommandResponse(cmdName, "Error occurred during auth",
                        HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        }
        return isGet ? command.runGet(zkServer, kwargs) : command.runPost(zkServer, inputStream);
    }

    private static List<Id> handleAuthentication(final HttpServletRequest request, final String authInfo)
            throws KeeperException.AuthFailedException {
        final String[] authData = authInfo.split(AUTH_INFO_SEPARATOR);
        // for IP and x509, auth info only contains the schema and Auth Id will be extracted from HTTP request
        if (authData.length != 1 && authData.length != 2) {
            LOG.warn("Invalid auth info length");
            throw new KeeperException.AuthFailedException();
        }

        final String schema = authData[0];
        final ServerAuthenticationProvider authProvider = ProviderRegistry.getServerProvider(schema);
        if (authProvider != null) {
            try {
                final byte[] auth = authData.length == 2 ? authData[1].getBytes(StandardCharsets.UTF_8) : null;
                final List<Id> ids = authProvider.handleAuthentication(request, auth);
                if (ids.isEmpty()) {
                    LOG.warn("Auth Id list is empty");
                    throw new KeeperException.AuthFailedException();
                }
                return ids;
            } catch (final RuntimeException e) {
                LOG.warn("Caught runtime exception from AuthenticationProvider", e);
                throw new KeeperException.AuthFailedException();
            }
        } else {
            LOG.warn("Auth provider not found for schema");
            throw new KeeperException.AuthFailedException();
        }
    }

    /**
     * Grant or deny authorization for a command by matching
     * request-provided credentials with the ACLs present on a node.
     *
     * @param zkServer the ZooKeeper server object.
     * @param ids      the credentials extracted from the Authorization header.
     * @param perm     the set of permission bits required by the command.
     * @param path     the ZooKeeper node path whose ACLs should be used
     *                 to satisfy the perm bits.
     * @throws KeeperException.NoAuthException if one or more perm
     *                                         bits could not be satisfied.
     */
    private static void handleAuthorization(final ZooKeeperServer zkServer,
                                            final List<Id> ids,
                                            final int perm,
                                            final String path)
            throws KeeperException.NoNodeException, KeeperException.NoAuthException {
        final DataNode dataNode = zkServer.getZKDatabase().getNode(path);
        if (dataNode == null) {
            throw new KeeperException.NoNodeException(path);
        }
        final List<ACL> acls = zkServer.getZKDatabase().aclForNode(dataNode);
        // Check the individual bits of perm.
        final int bitWidth = Integer.SIZE - Integer.numberOfLeadingZeros(perm);
        for (int b = 0; b < bitWidth; b++) {
            final int permBit = 1 << b;
            if ((perm & permBit) != 0) {
                zkServer.checkACL(null, acls, permBit, ids, path, null);
            }
        }
    }

    /**
     * Returns the primary names of all registered commands.
     */
    public static Set<String> getPrimaryNames() {
        return primaryNames;
    }

    /**
     * Returns the commands registered under cmdName with registerCommand, or
     * null if no command is registered with that name.
     */
    public static Command getCommand(String cmdName) {
        return commands.get(cmdName);
    }

    static {
        registerCommand(new CnxnStatResetCommand());
        registerCommand(new ConfCommand());
        registerCommand(new ConsCommand());
        registerCommand(new DigestCommand());
        registerCommand(new DirsCommand());
        registerCommand(new DumpCommand());
        registerCommand(new EnvCommand());
        registerCommand(new GetTraceMaskCommand());
        registerCommand(new InitialConfigurationCommand());
        registerCommand(new IsroCommand());
        registerCommand(new LastSnapshotCommand());
        registerCommand(new LeaderCommand());
        registerCommand(new MonitorCommand());
        registerCommand(new ObserverCnxnStatResetCommand());
        registerCommand(new RestoreCommand());
        registerCommand(new RuokCommand());
        registerCommand(new SetTraceMaskCommand());
        registerCommand(new SnapshotCommand());
        registerCommand(new SrvrCommand());
        registerCommand(new StatCommand());
        registerCommand(new StatResetCommand());
        registerCommand(new SyncedObserverConsCommand());
        registerCommand(new SystemPropertiesCommand());
        registerCommand(new VotingViewCommand());
        registerCommand(new WatchCommand());
        registerCommand(new WatchesByPathCommand());
        registerCommand(new WatchSummaryCommand());
        registerCommand(new ZabStateCommand());
    }

    /**
     * Reset all connection statistics.
     */
    public static class CnxnStatResetCommand extends GetCommand {

        public CnxnStatResetCommand() {
            super(Arrays.asList("connection_stat_reset", "crst"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            zkServer.getServerCnxnFactory().resetAllConnectionStats();
            return response;

        }

    }

    /**
     * Server configuration parameters.
     *
     * @see ZooKeeperServer#getConf()
     */
    public static class ConfCommand extends GetCommand {

        public ConfCommand() {
            super(Arrays.asList("configuration", "conf", "config"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.putAll(zkServer.getConf().toMap());
            return response;
        }

    }

    /**
     * Information on client connections to server. Returned Map contains:
     * - "connections": list of connection info objects
     *
     * @see org.apache.zookeeper.server.ServerCnxn#getConnectionInfo(boolean)
     */
    public static class ConsCommand extends GetCommand {

        public ConsCommand() {
            super(Arrays.asList("connections", "cons"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            ServerCnxnFactory serverCnxnFactory = zkServer.getServerCnxnFactory();
            if (serverCnxnFactory != null) {
                response.put("connections", serverCnxnFactory.getAllConnectionInfo(false));
            } else {
                response.put("connections", Collections.emptyList());
            }
            ServerCnxnFactory secureServerCnxnFactory = zkServer.getSecureServerCnxnFactory();
            if (secureServerCnxnFactory != null) {
                response.put("secure_connections", secureServerCnxnFactory.getAllConnectionInfo(false));
            } else {
                response.put("secure_connections", Collections.emptyList());
            }
            return response;
        }

    }

    /**
     * Information on ZK datadir and snapdir size in bytes.
     */
    public static class DirsCommand extends GetCommand {

        public DirsCommand() {
            super(Arrays.asList("dirs"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("datadir_size", zkServer.getDataDirSize());
            response.put("logdir_size", zkServer.getLogDirSize());
            return response;
        }

    }

    /**
     * Information on session expirations and ephemerals. Returned map contains:
     * - "expiry_time_to_session_ids": Map&lt;Long, Set&lt;Long&gt;&gt;
     * time -&gt; sessions IDs of sessions that expire at time
     * - "session_id_to_ephemeral_paths": Map&lt;Long, Set&lt;String&gt;&gt;
     * session ID -&gt; ephemeral paths created by that session
     *
     * @see ZooKeeperServer#getSessionExpiryMap()
     * @see ZooKeeperServer#getEphemerals()
     */
    public static class DumpCommand extends GetCommand {

        public DumpCommand() {
            super(Arrays.asList("dump"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("expiry_time_to_session_ids", zkServer.getSessionExpiryMap());
            response.put("session_id_to_ephemeral_paths", zkServer.getEphemerals());
            return response;
        }

    }

    /**
     * All defined environment variables.
     */
    public static class EnvCommand extends GetCommand {

        public EnvCommand() {
            super(Arrays.asList("environment", "env", "envi"), false);
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            for (Entry e : Environment.list()) {
                response.put(e.getKey(), e.getValue());
            }
            return response;
        }

    }

    /**
     * Digest histories for every specific number of txns.
     */
    public static class DigestCommand extends GetCommand {

        public DigestCommand() {
            super(Arrays.asList("hash"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("digests", zkServer.getZKDatabase().getDataTree().getDigestLog());
            return response;
        }

    }

    /**
     * The current trace mask. Returned map contains:
     * - "tracemask": Long
     */
    public static class GetTraceMaskCommand extends GetCommand {

        public GetTraceMaskCommand() {
            super(Arrays.asList("get_trace_mask", "gtmk"), false);
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("tracemask", ZooTrace.getTextTraceLevel());
            return response;
        }

    }

    public static class InitialConfigurationCommand extends GetCommand {

        public InitialConfigurationCommand() {
            super(Arrays.asList("initial_configuration", "icfg"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("initial_configuration", zkServer.getInitialConfig());
            return response;
        }

    }

    /**
     * Is this server in read-only mode. Returned map contains:
     * - "is_read_only": Boolean
     */
    public static class IsroCommand extends GetCommand {

        public IsroCommand() {
            super(Arrays.asList("is_read_only", "isro"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("read_only", zkServer instanceof ReadOnlyZooKeeperServer);
            return response;
        }

    }

    /**
     * Command returns information of the last snapshot that zookeeper server
     * has finished saving to disk. During the time between the server starts up
     * and it finishes saving its first snapshot, the command returns the zxid
     * and last modified time of the snapshot file used for restoration at
     * server startup. Returned map contains:
     * - "zxid": String
     * - "timestamp": Long
     */
    public static class LastSnapshotCommand extends GetCommand {

        public LastSnapshotCommand() {
            super(Arrays.asList("last_snapshot", "lsnp"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            SnapshotInfo info = zkServer.getTxnLogFactory().getLastSnapshotInfo();
            response.put("zxid", Long.toHexString(info == null ? -1L : info.zxid));
            response.put("timestamp", info == null ? -1L : info.timestamp);
            return response;
        }

    }

    /**
     * Returns the leader status of this instance and the leader host string.
     */
    public static class LeaderCommand extends GetCommand {

        public LeaderCommand() {
            super(Arrays.asList("leader", "lead"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            if (zkServer instanceof QuorumZooKeeperServer) {
                response.put("is_leader", zkServer instanceof LeaderZooKeeperServer);
                QuorumPeer peer = ((QuorumZooKeeperServer) zkServer).self;
                response.put("leader_id", peer.getLeaderId());
                String leaderAddress = peer.getLeaderAddress();
                response.put("leader_ip", leaderAddress != null ? leaderAddress : "");
            } else {
                response.put("error", "server is not initialized");
            }
            return response;
        }

    }

    /**
     * Some useful info for monitoring. Returned map contains:
     * - "version": String
     * server version
     * - "avg_latency": Long
     * - "max_latency": Long
     * - "min_latency": Long
     * - "packets_received": Long
     * - "packets_sents": Long
     * - "num_alive_connections": Integer
     * - "outstanding_requests": Long
     * number of unprocessed requests
     * - "server_state": "leader", "follower", or "standalone"
     * - "znode_count": Integer
     * - "watch_count": Integer
     * - "ephemerals_count": Integer
     * - "approximate_data_size": Long
     * - "open_file_descriptor_count": Long (unix only)
     * - "max_file_descriptor_count": Long (unix only)
     * - "fsync_threshold_exceed_count": Long
     * - "non_mtls_conn_count": Long
     * - "non_mtls_remote_conn_count": Long
     * - "non_mtls_local_conn_count": Long
     * - "followers": Integer (leader only)
     * - "synced_followers": Integer (leader only)
     * - "pending_syncs": Integer (leader only)
     */
    public static class MonitorCommand extends GetCommand {

        public MonitorCommand() {
            super(Arrays.asList("monitor", "mntr"), false);
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            zkServer.dumpMonitorValues(response::put);
            ServerMetrics.getMetrics().getMetricsProvider().dump(response::put);
            return response;

        }

    }

    /**
     * Reset all observer connection statistics.
     */
    public static class ObserverCnxnStatResetCommand extends GetCommand {

        public ObserverCnxnStatResetCommand() {
            super(Arrays.asList("observer_connection_stat_reset", "orst"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            if (zkServer instanceof LeaderZooKeeperServer) {
                Leader leader = ((LeaderZooKeeperServer) zkServer).getLeader();
                leader.resetObserverConnectionStats();
            } else if (zkServer instanceof FollowerZooKeeperServer) {
                Follower follower = ((FollowerZooKeeperServer) zkServer).getFollower();
                follower.resetObserverConnectionStats();
            }
            return response;
        }

    }

    /**
     * Restore from snapshot on the current server.
     *
     * Returned map contains:
     * - "last_zxid": String
     */
    public static class RestoreCommand extends PostCommand {
        static final String RESPONSE_DATA_LAST_ZXID = "last_zxid";
        static final String ADMIN_RESTORE_ENABLED = "zookeeper.admin.restore.enabled";

        private RateLimiter rateLimiter;

        public RestoreCommand() {
            super(Arrays.asList("restore", "rest"), true, new AuthRequest(ZooDefs.Perms.ALL, ROOT_PATH));
            rateLimiter = new RateLimiter(1, rateLimiterInterval, TimeUnit.MILLISECONDS);
        }

        @Override
        public CommandResponse runPost(final ZooKeeperServer zkServer, final InputStream inputStream) {
            final CommandResponse response = initializeResponse();

            // check feature flag
            final boolean restoreEnabled = Boolean.parseBoolean(System.getProperty(ADMIN_RESTORE_ENABLED, "true"));
            if (!restoreEnabled) {
                response.setStatusCode(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                LOG.warn("Restore command is disabled");
                return response;
            }

            if (!zkServer.isSerializeLastProcessedZxidEnabled()) {
                response.setStatusCode(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                LOG.warn("Restore command requires serializeLastProcessedZxidEnable flag is set to true");
                return response;
            }

            if (inputStream == null) {
                response.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
                LOG.warn("InputStream from restore request is null");
                return response;
            }

            // check rate limiting
            if (!rateLimiter.allow()) {
                response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS_429);
                ServerMetrics.getMetrics().RESTORE_RATE_LIMITED_COUNT.add(1);
                LOG.warn("Restore request was rate limited");
                return response;
            }

            // restore from snapshot InputStream
            try {
                final long lastZxid = zkServer.restoreFromSnapshot(inputStream);
                response.put(RESPONSE_DATA_LAST_ZXID, lastZxid);
            } catch (final Exception e) {
                response.setStatusCode(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                ServerMetrics.getMetrics().RESTORE_ERROR_COUNT.add(1);
                LOG.warn("Exception occurred when restore snapshot via the restore command", e);
            }
            return response;
        }
    }

    /**
     * No-op command, check if the server is running.
     */
    public static class RuokCommand extends GetCommand {

        public RuokCommand() {
            super(Arrays.asList("ruok"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            return initializeResponse();
        }

    }

    /**
     * Sets the trace mask. Required arguments:
     * - "traceMask": Long
     * Returned Map contains:
     * - "tracemask": Long
     */
    public static class SetTraceMaskCommand extends GetCommand {

        public SetTraceMaskCommand() {
            super(Arrays.asList("set_trace_mask", "stmk"), false);
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            long traceMask;
            if (!kwargs.containsKey("traceMask")) {
                response.put("error", "setTraceMask requires long traceMask argument");
                return response;
            }
            try {
                traceMask = Long.parseLong(kwargs.get("traceMask"));
            } catch (NumberFormatException e) {
                response.put("error", "setTraceMask requires long traceMask argument, got " + kwargs.get("traceMask"));
                return response;
            }

            ZooTrace.setTextTraceLevel(traceMask);
            response.put("tracemask", traceMask);
            return response;
        }

    }

    /**
     * Take a snapshot of current server and stream out the data.
     *
     * Argument:
     * - "streaming": optional String to indicate whether streaming out data
     *
     * Returned snapshot as stream if streaming is true and metadata of the snapshot
     * - "last_zxid": String
     * - "snapshot_size": String
     */
    public static class SnapshotCommand extends GetCommand {
        static final String REQUEST_QUERY_PARAM_STREAMING = "streaming";

        static final String RESPONSE_HEADER_LAST_ZXID = "last_zxid";
        static final String RESPONSE_HEADER_SNAPSHOT_SIZE = "snapshot_size";

        static final String ADMIN_SNAPSHOT_ENABLED = "zookeeper.admin.snapshot.enabled";

        private final RateLimiter rateLimiter;

        public SnapshotCommand() {
            super(Arrays.asList("snapshot", "snap"), true, new AuthRequest(ZooDefs.Perms.ALL, ROOT_PATH));
            rateLimiter = new RateLimiter(1, rateLimiterInterval, TimeUnit.MICROSECONDS);
        }

        @SuppressFBWarnings(value = "OBL_UNSATISFIED_OBLIGATION",
                justification = "FileInputStream is passed to CommandResponse and closed in StreamOutputter")
        @Override
        public CommandResponse runGet(final ZooKeeperServer zkServer, final Map<String, String> kwargs) {
            final CommandResponse response = initializeResponse();

            // check feature flag
            final boolean snapshotEnabled = Boolean.parseBoolean(System.getProperty(ADMIN_SNAPSHOT_ENABLED, "true"));
            if (!snapshotEnabled) {
                response.setStatusCode(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                LOG.warn("Snapshot command is disabled");
                return response;
            }

            if (!zkServer.isSerializeLastProcessedZxidEnabled()) {
                response.setStatusCode(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                LOG.warn("Snapshot command requires serializeLastProcessedZxidEnable flag is set to true");
                return response;
            }

            // check rate limiting
            if (!rateLimiter.allow()) {
                response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS_429);
                ServerMetrics.getMetrics().SNAPSHOT_RATE_LIMITED_COUNT.add(1);
                LOG.warn("Snapshot request was rate limited");
                return response;
            }

            // check the streaming query param
            boolean streaming = true;
            if (kwargs.containsKey(REQUEST_QUERY_PARAM_STREAMING)) {
                streaming = Boolean.parseBoolean(kwargs.get(REQUEST_QUERY_PARAM_STREAMING));
            }

            // take snapshot and stream out data if needed
            try {
                final File snapshotFile = zkServer.takeSnapshot(false, false);
                final long lastZxid = Util.getZxidFromName(snapshotFile.getName(), SNAPSHOT_FILE_PREFIX);
                response.addHeader(RESPONSE_HEADER_LAST_ZXID, "0x" + ZxidUtils.zxidToString(lastZxid));

                final long size = snapshotFile.length();
                response.addHeader(RESPONSE_HEADER_SNAPSHOT_SIZE, String.valueOf(size));

                if (size == 0) {
                    response.setStatusCode(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    ServerMetrics.getMetrics().SNAPSHOT_ERROR_COUNT.add(1);
                    LOG.warn("Snapshot file {} is empty", snapshotFile);
                } else if (streaming) {
                    response.setInputStream(new FileInputStream(snapshotFile));
                }
            } catch (final Exception e) {
                response.setStatusCode(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                ServerMetrics.getMetrics().SNAPSHOT_ERROR_COUNT.add(1);
                LOG.warn("Exception occurred when taking the snapshot via the snapshot admin command", e);
            }
            return response;
        }
    }

    /**
     * Server information. Returned map contains:
     * - "version": String
     * version of server
     * - "read_only": Boolean
     * is server in read-only mode
     * - "server_stats": ServerStats object
     * - "node_count": Integer
     */
    public static class SrvrCommand extends GetCommand {

        public SrvrCommand() {
            super(Arrays.asList("server_stats", "srvr"));
        }

        // Allow subclasses (e.g. StatCommand) to specify their own names
        protected SrvrCommand(List<String> names) {
            super(names);
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            LOG.info("running stat");
            response.put("version", Version.getFullVersion());
            response.put("read_only", zkServer instanceof ReadOnlyZooKeeperServer);
            response.put("server_stats", zkServer.serverStats());
            response.put("client_response", zkServer.serverStats().getClientResponseStats());
            if (zkServer instanceof LeaderZooKeeperServer) {
                Leader leader = ((LeaderZooKeeperServer) zkServer).getLeader();
                response.put("proposal_stats", leader.getProposalStats());
            }
            response.put("node_count", zkServer.getZKDatabase().getNodeCount());
            return response;
        }

    }

    /**
     * Same as SrvrCommand but has extra "connections" entry.
     */
    public static class StatCommand extends SrvrCommand {

        public StatCommand() {
            super(Arrays.asList("stats", "stat"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = super.runGet(zkServer, kwargs);

            final Iterable<Map<String, Object>> connections;
            if (zkServer.getServerCnxnFactory() != null) {
                connections = zkServer.getServerCnxnFactory().getAllConnectionInfo(true);
            } else {
                connections = Collections.emptyList();
            }
            response.put("connections", connections);

            final Iterable<Map<String, Object>> secureConnections;
            if (zkServer.getSecureServerCnxnFactory() != null) {
                secureConnections = zkServer.getSecureServerCnxnFactory().getAllConnectionInfo(true);
            } else {
                secureConnections = Collections.emptyList();
            }
            response.put("secure_connections", secureConnections);
            return response;
        }

    }

    /**
     * Resets server statistics.
     */
    public static class StatResetCommand extends GetCommand {

        public StatResetCommand() {
            super(Arrays.asList("stat_reset", "srst"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            zkServer.serverStats().reset();
            return response;
        }

    }

    /**
     * Information on observer connections to server. Returned Map contains:
     * - "synced_observers": Integer (leader/follower only)
     * - "observers": list of observer learner handler info objects (leader/follower only)
     *
     * @see org.apache.zookeeper.server.quorum.LearnerHandler#getLearnerHandlerInfo()
     */
    public static class SyncedObserverConsCommand extends GetCommand {

        public SyncedObserverConsCommand() {
            super(Arrays.asList("observers", "obsr"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {

            CommandResponse response = initializeResponse();

            if (zkServer instanceof LeaderZooKeeperServer) {
                Leader leader = ((LeaderZooKeeperServer) zkServer).getLeader();

                response.put("synced_observers", leader.getObservingLearners().size());
                response.put("observers", leader.getObservingLearnersInfo());
                return response;
            } else if (zkServer instanceof FollowerZooKeeperServer) {
                Follower follower = ((FollowerZooKeeperServer) zkServer).getFollower();
                Integer syncedObservers = follower.getSyncedObserverSize();
                if (syncedObservers != null) {
                    response.put("synced_observers", syncedObservers);
                    response.put("observers", follower.getSyncedObserversInfo());
                    return response;
                }
            }

            response.put("synced_observers", 0);
            response.put("observers", Collections.emptySet());
            return response;
        }

    }

    /**
     * All defined system properties.
     */
    public static class SystemPropertiesCommand extends GetCommand {

        public SystemPropertiesCommand() {
            super(Arrays.asList("system_properties", "sysp"), false);
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            Properties systemProperties = System.getProperties();
            SortedMap<String, String> sortedSystemProperties = new TreeMap<>();
            systemProperties.forEach((k, v) -> sortedSystemProperties.put(k.toString(), v.toString()));
            response.putAll(sortedSystemProperties);
            return response;
        }

    }

    /**
     * Returns the current ensemble configuration information.
     * It provides list of current voting members in the ensemble.
     */
    public static class VotingViewCommand extends GetCommand {

        public VotingViewCommand() {
            super(Arrays.asList("voting_view"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            if (zkServer instanceof QuorumZooKeeperServer) {
                QuorumPeer peer = ((QuorumZooKeeperServer) zkServer).self;
                Map<Long, QuorumServerView> votingView = peer.getVotingView().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new QuorumServerView(e.getValue())));
                response.put("current_config", votingView);
            } else {
                response.put("current_config", Collections.emptyMap());
            }
            return response;
        }

        @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "class is used only for JSON serialization")
        private static class QuorumServerView {

            @JsonProperty
            private List<String> serverAddresses;

            @JsonProperty
            private List<String> electionAddresses;

            @JsonProperty
            private String clientAddress;

            @JsonProperty
            private String learnerType;

            public QuorumServerView(QuorumPeer.QuorumServer quorumServer) {
                this.serverAddresses = getMultiAddressString(quorumServer.addr);
                this.electionAddresses = getMultiAddressString(quorumServer.electionAddr);
                this.learnerType = quorumServer.type.equals(LearnerType.PARTICIPANT) ? "participant" : "observer";
                this.clientAddress = getAddressString(quorumServer.clientAddr);
            }

            private static List<String> getMultiAddressString(MultipleAddresses multipleAddresses) {
                if (multipleAddresses == null) {
                    return Collections.emptyList();
                }

                return multipleAddresses.getAllAddresses().stream()
                        .map(QuorumServerView::getAddressString)
                        .collect(Collectors.toList());
            }

            private static String getAddressString(InetSocketAddress address) {
                if (address == null) {
                    return "";
                }
                return String.format("%s:%d", QuorumPeer.QuorumServer.delimitedHostString(address), address.getPort());
            }
        }

    }

    /**
     * Watch information aggregated by session. Returned Map contains:
     * - "session_id_to_watched_paths": Map&lt;Long, Set&lt;String&gt;&gt; session ID -&gt; watched paths
     *
     * @see DataTree#getWatches()
     * @see DataTree#getWatches()
     */
    public static class WatchCommand extends GetCommand {

        public WatchCommand() {
            super(Arrays.asList("watches", "wchc"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.put("session_id_to_watched_paths", dt.getWatches().toMap());
            return response;
        }

    }

    /**
     * Watch information aggregated by path. Returned Map contains:
     * - "path_to_session_ids": Map&lt;String, Set&lt;Long&gt;&gt; path -&gt; session IDs of sessions watching path
     *
     * @see DataTree#getWatchesByPath()
     */
    public static class WatchesByPathCommand extends GetCommand {

        public WatchesByPathCommand() {
            super(Arrays.asList("watches_by_path", "wchp"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.put("path_to_session_ids", dt.getWatchesByPath().toMap());
            return response;
        }

    }

    /**
     * Summarized watch information.
     *
     * @see DataTree#getWatchesSummary()
     */
    public static class WatchSummaryCommand extends GetCommand {

        public WatchSummaryCommand() {
            super(Arrays.asList("watch_summary", "wchs"));
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.putAll(dt.getWatchesSummary().toMap());
            return response;
        }

    }

    /**
     * Returns the current phase of Zab protocol that peer is running.
     * It can be in one of these phases: ELECTION, DISCOVERY, SYNCHRONIZATION, BROADCAST
     */
    public static class ZabStateCommand extends GetCommand {

        public ZabStateCommand() {
            super(Arrays.asList("zabstate"), false);
        }

        @Override
        public CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            if (zkServer instanceof QuorumZooKeeperServer) {
                QuorumPeer peer = ((QuorumZooKeeperServer) zkServer).self;
                QuorumPeer.ZabState zabState = peer.getZabState();
                QuorumVerifier qv = peer.getQuorumVerifier();

                QuorumPeer.QuorumServer voter = qv.getVotingMembers().get(peer.getMyId());
                boolean voting = (
                        voter != null
                                && voter.addr.equals(peer.getQuorumAddress())
                                && voter.electionAddr.equals(peer.getElectionAddress())
                );
                response.put("myid", zkServer.getConf().getServerId());
                response.put("is_leader", zkServer instanceof LeaderZooKeeperServer);
                response.put("quorum_address", peer.getQuorumAddress());
                response.put("election_address", peer.getElectionAddress());
                response.put("client_address", peer.getClientAddress());
                response.put("voting", voting);
                long lastProcessedZxid = zkServer.getZKDatabase().getDataTreeLastProcessedZxid();
                response.put("last_zxid", "0x" + ZxidUtils.zxidToString(lastProcessedZxid));
                response.put("zab_epoch", ZxidUtils.getEpochFromZxid(lastProcessedZxid));
                response.put("zab_counter", ZxidUtils.getCounterFromZxid(lastProcessedZxid));
                response.put("zabstate", zabState.name().toLowerCase());
            } else {
                response.put("voting", false);
                response.put("zabstate", "");
            }
            return response;
        }

    }

    private Commands() {
    }

}
