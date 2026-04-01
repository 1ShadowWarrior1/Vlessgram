package org.telegram.vless;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONArray;
import org.json.JSONObject;
import org.telegram.messenger.AndroidUtilities;
import org.telegram.messenger.ApplicationLoader;
import org.telegram.messenger.MessagesController;
import org.telegram.messenger.NotificationCenter;
import org.telegram.messenger.SharedConfig;
import org.telegram.tgnet.ConnectionsManager;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stores VLESS/Trojan/SS nodes, fetches subscriptions, runs ping loop and controls SOCKS proxy integration.
 */
public final class VlessRepository {
    public interface Listener {
        void onVlessDataChanged();
    }

    public static final class VlessNode {
        public final String uri;
        public String name;
        public final boolean manual;
        public final String subscriptionId; // null for manual nodes

        public final SharedConfig.ProxyInfo proxyInfo;

        private VlessNode(String uri, String name, boolean manual, String subscriptionId, int localPort) {
            this.uri = uri;
            this.name = name;
            this.manual = manual;
            this.subscriptionId = subscriptionId;
            this.proxyInfo = new SharedConfig.ProxyInfo("127.0.0.1", localPort, "", "", "");
            this.proxyInfo.available = false; // Unknown until ping check
            this.proxyInfo.checking = false;
            this.proxyInfo.ping = -1; // Unknown ping until checked
        }
    }

    private static final String PREFS_NAME = "vless_data";
    private static final String KEY_DATA_JSON = "data_json";

    private static final String[] USER_AGENTS = new String[]{
            "clash-verge/v1.3.8",
            "v2rayNG/1.8.5",
            "Hiddify/2.0.5",
            "Shadowrocket/1.0",
            "v2rayN/6.23",
            "clash-meta"
    };
    private static final String DEFAULT_USER_AGENT = "Mozilla/5.0";

    private static VlessRepository instance;

    public static synchronized VlessRepository getInstance() {
        if (instance == null) {
            instance = new VlessRepository(ApplicationLoader.applicationContext);
        }
        return instance;
    }

    private final Context appContext;
    private final SharedPreferences prefs;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();
    private final ExecutorService coreExecutor = Executors.newSingleThreadExecutor();
    /** Separate from coreExecutor so subscription HTTP fetches don't delay core start/stop. */
    private final ExecutorService networkExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService pingExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService pingWorkers = Executors.newFixedThreadPool(6);

    private final Object lock = new Object();

    private final List<VlessNode> manualNodes = new ArrayList<>();
    private final List<VlessSubscription> subscriptions = new ArrayList<>();

    private String activeUri = "";

    private boolean autoEnableVpn = true;
    private boolean autoSwitchVpn = true;
    private boolean sortByPing = false;

    private volatile int localPort = 20808;
    private volatile boolean coreRunning = false;

    private volatile boolean lastActiveAvailable = true;
    private volatile long lastAutoSwitchTsMs = 0;
    private final long autoSwitchCooldownMs = 10_000; // 10 seconds cooldown
    private volatile boolean isSwitching = false; // Prevent multiple simultaneous switches

    private final AtomicBoolean autoConnectStarted = new AtomicBoolean(false);

    private VlessRepository(Context context) {
        this.appContext = context.getApplicationContext();
        this.prefs = appContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);

        loadFromPrefs();
        startPingLoop();
        // Start periodic subscription refresh every 15 minutes
        startPeriodicSubscriptionRefresh();
        // Warm up: extract libvless.so from assets without blocking coreExecutor.
        new Thread(() -> {
            try {
                VlessCoreManager.ensureExtractedSo(appContext);
            } catch (Throwable ignored) {
            }
        }, "vless-warmup").start();
    }

    public void addListener(Listener l) {
        if (l == null) return;
        listeners.addIfAbsent(l);
    }

    public void removeListener(Listener l) {
        if (l == null) return;
        listeners.remove(l);
    }

    public boolean isSortByPing() {
        return sortByPing;
    }

    public boolean getAutoEnableVpn() {
        return autoEnableVpn;
    }

    public void setAutoEnableVpn(boolean value) {
        synchronized (lock) {
            autoEnableVpn = value;
            saveToPrefsLocked();
        }
    }

    public boolean getAutoSwitchVpn() {
        return autoSwitchVpn;
    }

    public void setAutoSwitchVpn(boolean value) {
        synchronized (lock) {
            autoSwitchVpn = value;
            saveToPrefsLocked();
        }
    }

    /** Snapshot of a subscription for UI. */
    public static final class SubscriptionInfo {
        public final String id;
        public final String url;
        public final String name;
        public final List<VlessNode> nodes;

        SubscriptionInfo(String id, String url, String name, List<VlessNode> nodes) {
            this.id = id;
            this.url = url;
            this.name = name;
            this.nodes = new ArrayList<>(nodes);
        }
    }

    public List<SubscriptionInfo> getSubscriptions() {
        synchronized (lock) {
            List<SubscriptionInfo> out = new ArrayList<>();
            for (VlessSubscription s : subscriptions) {
                out.add(new SubscriptionInfo(s.id, s.url, s.name, s.nodes));
            }
            return out;
        }
    }

    public List<VlessNode> getManualNodes() {
        synchronized (lock) {
            return new ArrayList<>(manualNodes);
        }
    }

    public void deleteSubscriptionAsync(String subId, Runnable onDone) {
        coreExecutor.execute(() -> {
            if (subId == null) return;
            boolean needDisconnect = false;
            synchronized (lock) {
                for (VlessSubscription s : subscriptions) {
                    if (subId.equals(s.id)) {
                        for (VlessNode n : s.nodes) {
                            if (n.uri.equals(activeUri)) {
                                needDisconnect = true;
                                break;
                            }
                        }
                        subscriptions.remove(s);
                        saveToPrefsLocked();
                        break;
                    }
                }
            }
            if (needDisconnect) {
                synchronized (lock) {
                    activeUri = "";
                    coreRunning = false;
                }
                disconnectInternal();
            }
            notifyChanged();
            if (onDone != null) AndroidUtilities.runOnUIThread(onDone);
        });
    }

    public void updateSubscriptionAsync(String subId, Runnable onDone) {
        networkExecutor.execute(() -> {
            if (subId == null) return;
            String url = null;
            synchronized (lock) {
                for (VlessSubscription s : subscriptions) {
                    if (subId.equals(s.id)) {
                        url = s.url;
                        break;
                    }
                }
            }
            if (url != null) {
                if (addOrUpdateSubscriptionInternal(url)) {
                    notifyChanged();
                }
            }
            if (onDone != null) {
                AndroidUtilities.runOnUIThread(onDone);
            }
        });
    }

    public void toggleSortByPing() {
        synchronized (lock) {
            sortByPing = !sortByPing;
            saveToPrefs();
        }
        notifyChanged();
    }

    public String getActiveUri() {
        synchronized (lock) {
            return activeUri;
        }
    }

    public int getLocalPort() {
        return localPort;
    }

    public List<VlessNode> getAllNodesSortedForUi() {
        List<VlessNode> nodes = getAllNodesFlattened();
        boolean sort;
        synchronized (lock) {
            sort = sortByPing;
        }
        if (!sort) {
            return nodes;
        }

        nodes.sort(Comparator.comparingLong((VlessNode n) -> n.proxyInfo.ping >= 0 ? n.proxyInfo.ping : 9999)
                .thenComparing(n -> n.name == null ? "" : n.name));
        return nodes;
    }

    public VlessNode findNodeByUri(String uri) {
        if (uri == null) return null;
        synchronized (lock) {
            for (VlessNode n : manualNodes) {
                if (uri.equals(n.uri)) return n;
            }
            for (VlessSubscription s : subscriptions) {
                for (VlessNode n : s.nodes) {
                    if (uri.equals(n.uri)) return n;
                }
            }
        }
        return null;
    }

    public void ensureAutoConnect() {
        if (autoConnectStarted.getAndSet(true)) {
            return;
        }
        if (!autoEnableVpn) return;
        String uri;
        synchronized (lock) {
            uri = activeUri;
        }
        if (uri == null || uri.isEmpty()) return;

        // Fast connect on app startup with immediate failover
        coreExecutor.execute(() -> {
            try {
                VlessNode node = findNodeByUri(uri);
                if (node != null) {
                    android.util.Log.i("VlessRepository", "VLESS: Auto-connecting to \"" + (node.name != null ? node.name : uri) + "\"");
                    connectInternal(uri);
                }
            } catch (Throwable ignored) {
            }
        });
    }

    public void checkPingAllAsync() {
        pingExecutor.execute(() -> {
            // We don't need to stop the core; just perform one ping sweep.
            pingOnce();
        });
    }

    public void addManualKeyAsync(String uri, Runnable onDone) {
        coreExecutor.execute(() -> {
            String trimmed = uri == null ? "" : uri.trim();
            if (!(trimmed.startsWith("vless://") || trimmed.startsWith("trojan://") || trimmed.startsWith("ss://"))) {
                return;
            }
            VlessConfigParser.ParsedConfig pc = VlessConfigParser.parseToLibConfigJson(trimmed, 1080);
            if (pc == null) return;

            synchronized (lock) {
                // De-dup by uri.
                VlessNode existing = findNodeByUri(trimmed);
                if (existing != null) {
                    existing.name = pc.name;
                } else {
                    manualNodes.add(new VlessNode(trimmed, pc.name, true, null, localPort));
                }
                saveToPrefsLocked();
            }
            notifyChanged();
            if (onDone != null) {
                AndroidUtilities.runOnUIThread(onDone);
            }
        });
    }

    public void addOrUpdateSubscriptionAsync(String url, Runnable onDone) {
        networkExecutor.execute(() -> {
            if (addOrUpdateSubscriptionInternal(url)) {
                notifyChanged();
            }
            if (onDone != null) {
                AndroidUtilities.runOnUIThread(onDone);
            }
        });
    }

    /** Refresh all saved subscriptions in background (used on app launch). */
    public void refreshAllSubscriptionsAsync() {
        networkExecutor.execute(() -> {
            List<String> urls = new ArrayList<>();
            synchronized (lock) {
                for (VlessSubscription s : subscriptions) {
                    if (s.url != null && !s.url.trim().isEmpty()) {
                        urls.add(s.url);
                    }
                }
            }
            if (urls.isEmpty()) {
                return;
            }

            boolean changed = false;
            for (String url : urls) {
                try {
                    if (addOrUpdateSubscriptionInternal(url)) {
                        changed = true;
                    }
                } catch (Exception e) {
                    // Ignore individual subscription errors, continue with others
                }
            }
            if (changed) {
                notifyChanged();
            }
        });
    }

    /** Refresh all saved subscriptions with delay after app launch. */
    public void refreshAllSubscriptionsAsyncDelayed() {
        pingExecutor.schedule(() -> {
            refreshAllSubscriptionsAsync();
        }, 3, TimeUnit.SECONDS);
    }

    public void deleteManualKeyAsync(String uri, Runnable onDone) {
        coreExecutor.execute(() -> {
            if (uri == null) return;
            synchronized (lock) {
                manualNodes.removeIf(n -> uri.equals(n.uri));
                if (uri.equals(activeUri)) {
                    activeUri = "";
                    coreRunning = false;
                    disconnectInternal();
                }
                saveToPrefsLocked();
            }
            notifyChanged();
            if (onDone != null) AndroidUtilities.runOnUIThread(onDone);
        });
    }

    public void toggleConnectAsync(String uri) {
        coreExecutor.execute(() -> {
            if (uri == null) return;
            String trimmed = uri.trim();
            synchronized (lock) {
                if (trimmed.equals(activeUri) && coreRunning) {
                    activeUri = "";
                    coreRunning = false;
                    disconnectInternal();
                    saveToPrefsLocked();
                    notifyChanged();
                    return;
                }
            }
            connectInternal(trimmed);
        });
    }

    public void disconnectAsync() {
        coreExecutor.execute(() -> {
            synchronized (lock) {
                if (activeUri == null || activeUri.isEmpty()) {
                    return;
                }
                activeUri = "";
                coreRunning = false;
                disconnectInternal();
                saveToPrefsLocked();
            }
            notifyChanged();
        });
    }

    private void connectInternal(String uri) {
        connectInternal(uri, null);
    }

    private void connectInternal(String uri, Runnable onFailCallback) {
        if (uri == null || uri.isEmpty()) return;
        String normalizedUri = normalizeNodeUri(uri);
        if (normalizedUri.isEmpty()) return;
        VlessNode node = findNodeByUri(normalizedUri);
        if (node == null) return;

        VlessConfigParser.ParsedConfig pc = VlessConfigParser.parseToLibConfigJson(normalizedUri, localPort);
        if (pc == null) {
            if (onFailCallback != null) {
                AndroidUtilities.runOnUIThread(onFailCallback);
            }
            return;
        }

        // Stop previous core (and local proxy).
        disconnectInternal();

        int port = VlessCoreManager.allocateFreePort();
        synchronized (lock) {
            localPort = port;
            // Update all nodes proxy ports.
            for (VlessNode n : manualNodes) {
                n.proxyInfo.port = port;
            }
            for (VlessSubscription s : subscriptions) {
                for (VlessNode n : s.nodes) {
                    n.proxyInfo.port = port;
                }
            }
        }

        pc = VlessConfigParser.parseToLibConfigJson(normalizedUri, port);
        if (pc == null) {
            if (onFailCallback != null) {
                AndroidUtilities.runOnUIThread(onFailCallback);
            }
            return;
        }

        // Turn on Telegram proxy immediately so MTProto switches before core download / SOCKS bind
        // (connections retry until the local port is accepting).
        enableTelegramProxy(node);

        // Setup connection timeout - 4 seconds for Reality protocol (needs more time)
        final long connectStartTime = System.currentTimeMillis();
        final long connectTimeout = 4000; // 4 seconds - Reality needs more time
        final int finalPort = port;
        final String nodeName = node.name != null ? node.name : (node.uri.length() > 20 ? node.uri.substring(0, 20) + "..." : node.uri);

        // Monitor core startup with timeout in background thread (NOT on UI thread!)
        coreExecutor.execute(() -> {
            boolean vpnConnected = false;
            long elapsed = 0; // Initialize to avoid compilation error
            
            // Check every 150ms for up to 4 seconds
            do {
                try {
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                elapsed = System.currentTimeMillis() - connectStartTime;

                // Check if SOCKS port is accepting connections (actual VPN connectivity)
                vpnConnected = isSocksPortAcceptingConnections("127.0.0.1", finalPort);

                if (vpnConnected) {
                    break;
                }
            } while (elapsed < connectTimeout);

            final boolean finalVpnConnected = vpnConnected;
            final long finalElapsed = elapsed;
            
            AndroidUtilities.runOnUIThread(() -> {
                if (finalVpnConnected && finalElapsed < connectTimeout) {
                    // VPN connected successfully within timeout
                    android.util.Log.i("VlessRepository", "VLESS: Connected \"" + nodeName + "\" in " + finalElapsed + "ms");

                    synchronized (lock) {
                        activeUri = normalizedUri;
                        coreRunning = true;
                        saveToPrefsLocked();
                    }
                    synchronized (lock) {
                        lastActiveAvailable = true;
                    }
                    notifyChanged();
                } else {
                    // Timeout - switch to another VPN node (fast failover)
                    android.util.Log.w("VlessRepository", "VLESS: Timeout " + finalElapsed + "ms for \"" + nodeName + "\", switching");

                    // Auto-switch to best alternative VPN node (in background thread)
                    coreExecutor.execute(() -> {
                        disconnectInternal();

                        VlessNode best = pickBestAvailableAlternative(normalizedUri);
                        if (best != null) {
                            android.util.Log.i("VlessRepository", "VLESS: Auto-switch to \"" + (best.name != null ? best.name : "alternative") + "\" ping=" + best.proxyInfo.ping + "ms");
                            // Connect immediately with fast timeout
                            connectInternal(best.uri, null);
                        } else {
                            android.util.Log.w("VlessRepository", "VLESS: No alternative nodes available");
                        }

                        if (onFailCallback != null) {
                            AndroidUtilities.runOnUIThread(onFailCallback);
                        }
                    });
                }
            });
        });

        try {
            VlessCoreManager.startCore(appContext, pc.configJson, port);
        } catch (Exception e) {
            android.util.Log.e("VlessRepository", "VLESS: StartCore error: " + e.getMessage());
            
            // Preserve the URI so ensureAutoConnect retries on next launch.
            synchronized (lock) {
                activeUri = normalizedUri;
                coreRunning = false;
                saveToPrefsLocked();
            }
            disconnectInternal();
            
            // Try alternative VPN node (silent, no Toast)
            VlessNode best = pickBestAvailableAlternative(normalizedUri);
            if (best != null) {
                android.util.Log.i("VlessRepository", "VLESS: Error, auto-switch to \"" + (best.name != null ? best.name : "alternative") + "\"");
                coreExecutor.execute(() -> connectInternal(best.uri, null));
            }
            
            if (onFailCallback != null) {
                AndroidUtilities.runOnUIThread(onFailCallback);
            }
            notifyChanged();
            return;
        }
    }

    /**
     * Quick check if SOCKS port is accepting TCP connections.
     * Returns true if we can establish a TCP handshake within 100ms.
     */
    private boolean isSocksPortAcceptingConnections(String host, int port) {
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), 100);
            socket.setSoTimeout(100);

            // Try basic SOCKS5 greeting to verify it's actually a SOCKS proxy
            java.io.OutputStream out = socket.getOutputStream();
            java.io.InputStream in = socket.getInputStream();

            // SOCKS5: version=5, nmethods=1, no-auth
            out.write(new byte[]{0x05, 0x01, 0x00});
            out.flush();

            byte[] response = new byte[2];
            int read = in.read(response, 0, 2);

            // Valid SOCKS5 response: version=5, method=0 (no auth)
            return read >= 2 && response[0] == 0x05 && response[1] == 0x00;
        } catch (Exception e) {
            return false;
        }
    }

    private void enableTelegramProxy(VlessNode node) {
        if (node == null) return;

        // Remove previous local proxies (127.0.0.1) so only this node exists.
        removeLocalVlessProxies();

        SharedConfig.currentProxy = SharedConfig.addProxy(node.proxyInfo);

        // Update preferences: this is what ProxyListActivity checks.
        SharedPreferences mainPrefs = MessagesController.getGlobalMainSettings();
        SharedPreferences.Editor editor = mainPrefs.edit();
        editor.putBoolean("proxy_enabled", true);
        editor.putBoolean("proxy_enabled_calls", false);
        editor.putString("proxy_ip", node.proxyInfo.address);
        editor.putInt("proxy_port", node.proxyInfo.port);
        editor.putString("proxy_user", node.proxyInfo.username);
        editor.putString("proxy_pass", node.proxyInfo.password);
        editor.putString("proxy_secret", node.proxyInfo.secret);
        editor.apply();

        ConnectionsManager.setProxySettings(true,
                node.proxyInfo.address,
                node.proxyInfo.port,
                node.proxyInfo.username,
                node.proxyInfo.password,
                node.proxyInfo.secret);

        AndroidUtilities.runOnUIThread(() ->
                NotificationCenter.getGlobalInstance().postNotificationName(NotificationCenter.proxySettingsChanged));
    }

    private void disconnectInternal() {
        // Stop core first.
        VlessCoreManager.stopCore();

        // Remove all local vless proxies from proxy list.
        removeLocalVlessProxies();

        SharedConfig.currentProxy = null;

        // Disable proxy settings.
        SharedPreferences mainPrefs = MessagesController.getGlobalMainSettings();
        SharedPreferences.Editor editor = mainPrefs.edit();
        editor.putBoolean("proxy_enabled", false);
        editor.putBoolean("proxy_enabled_calls", false);
        editor.putString("proxy_ip", "");
        editor.putString("proxy_pass", "");
        editor.putString("proxy_user", "");
        editor.putInt("proxy_port", 1080);
        editor.putString("proxy_secret", "");
        editor.apply();

        ConnectionsManager.setProxySettings(false, "", 0, "", "", "");
        AndroidUtilities.runOnUIThread(() ->
                NotificationCenter.getGlobalInstance().postNotificationName(NotificationCenter.proxySettingsChanged));
    }

    private void removeLocalVlessProxies() {
        // Identify by host only (same as python example: 127.0.0.1 with empty auth/secret).
        for (int i = SharedConfig.proxyList.size() - 1; i >= 0; i--) {
            SharedConfig.ProxyInfo info = SharedConfig.proxyList.get(i);
            if (info != null && "127.0.0.1".equals(info.address)) {
                SharedConfig.deleteProxy(info);
            }
        }
    }

    private void pingOnce() {
        List<VlessNode> nodes;
        String active;
        boolean autoSwitch;
        final int socks5Port;
        final boolean running;
        synchronized (lock) {
            nodes = getAllNodesFlattenedLocked();
            active = activeUri;
            autoSwitch = autoSwitchVpn;
            socks5Port = localPort;
            running = coreRunning;
        }
        if (nodes.isEmpty()) return;

        android.util.Log.d("VlessRepository", "VLESS: pingOnce started, nodes=" + nodes.size() + ", active=" + active + ", autoSwitch=" + autoSwitch);

        CountDownLatch latch = new CountDownLatch(nodes.size());
        for (VlessNode n : nodes) {
            final boolean viaSocks = running && n.uri.equals(active);
            pingWorkers.execute(() -> {
                try {
                    updateNodePing(n, viaSocks, socks5Port);
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await(12, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        android.util.Log.d("VlessRepository", "VLESS: pingOnce completed");

        if (autoSwitch) {
            maybeAutoSwitch(active);
        }

        notifyChanged();
    }

    private void startPingLoop() {
        // Enable periodic ping check for auto-switch functionality
        // Checks if current VPN node is still available every 10 seconds (fast detection)
        pingExecutor.scheduleWithFixedDelay(() -> {
            try {
                pingOnce();
            } catch (Throwable ignored) {
            }
        }, 10, 30, TimeUnit.SECONDS);
    }

    private void startPeriodicSubscriptionRefresh() {
        pingExecutor.scheduleWithFixedDelay(() -> {
            try {
                refreshAllSubscriptionsAsync();
            } catch (Throwable ignored) {
            }
        }, 60, 900, TimeUnit.SECONDS); // Start after 60s, repeat every 15 minutes
    }

    private void maybeAutoSwitch(String activeBefore) {
        if (activeBefore == null || activeBefore.isEmpty()) {
            android.util.Log.d("VlessRepository", "VLESS: maybeAutoSwitch SKIP - no active URI");
            return;
        }
        VlessNode activeNode = findNodeByUri(activeBefore);
        if (activeNode == null) {
            android.util.Log.d("VlessRepository", "VLESS: maybeAutoSwitch SKIP - node not found for: " + activeBefore);
            return;
        }

        android.util.Log.i("VlessRepository", "VLESS: maybeAutoSwitch CHECK \"" + (activeNode.name != null ? activeNode.name : activeBefore) + "\" available=" + activeNode.proxyInfo.available + ", lastAvailable=" + lastActiveAvailable);

        boolean nowAvailable = activeNode.proxyInfo.available;
        if (nowAvailable) {
            lastActiveAvailable = true;
            android.util.Log.d("VlessRepository", "VLESS: maybeAutoSwitch - node is AVAILABLE, no switch needed");
            return;
        }

        // Trigger only on transition available -> unavailable.
        if (lastActiveAvailable) {
            long nowMs = System.currentTimeMillis();
            if (nowMs - lastAutoSwitchTsMs < autoSwitchCooldownMs) {
                android.util.Log.d("VlessRepository", "VLESS: Auto-switch cooldown, skipping (" + (nowMs - lastAutoSwitchTsMs) + "ms < " + autoSwitchCooldownMs + "ms)");
                lastActiveAvailable = false;
                return;
            }

            VlessNode best = pickBestAvailableAlternative(activeNode.uri);
            if (best != null) {
                lastAutoSwitchTsMs = nowMs;
                lastActiveAvailable = false;

                // Log the switch (no Toast to avoid UI lag)
                android.util.Log.i("VlessRepository", "VLESS: Auto-switch \"" +
                    (best.name != null ? best.name : activeNode.uri) +
                    "\" ping=" + best.proxyInfo.ping + "ms");

                // Connect asynchronously (silent switch)
                coreExecutor.execute(() -> {
                    android.util.Log.d("VlessRepository", "VLESS: Starting connectInternal for alternative");
                    connectInternal(best.uri);
                    // Notify UI on main thread to avoid RecyclerView crash
                    AndroidUtilities.runOnUIThread(() -> {
                        NotificationCenter.getGlobalInstance().postNotificationName(
                            NotificationCenter.proxySettingsChanged
                        );
                    });
                });
            } else {
                lastActiveAvailable = false;
                android.util.Log.w("VlessRepository", "VLESS: No alternative for \"" + activeBefore + "\"");
            }
        } else {
            lastActiveAvailable = false;
            android.util.Log.d("VlessRepository", "VLESS: maybeAutoSwitch - already marked as unavailable");
        }
    }

    private VlessNode pickBestAvailableAlternative(String activeUri) {
        List<VlessNode> nodes = getAllNodesFlattened();
        VlessNode best = null;
        long bestPing = Long.MAX_VALUE;
        
        android.util.Log.d("VlessRepository", "VLESS: pickBestAvailableAlternative for \"" + activeUri + "\", total nodes: " + nodes.size());
        
        for (VlessNode n : nodes) {
            if (activeUri != null && activeUri.equals(n.uri)) {
                android.util.Log.d("VlessRepository", "VLESS: Skip current active: " + n.uri);
                continue;
            }

            // Prefer nodes with known good ping, but also consider unknown nodes (ping = -1)
            // as potential alternatives (they might be available)
            long p = n.proxyInfo.ping;
            
            String nodeName = n.name != null ? n.name : n.uri.substring(0, Math.min(30, n.uri.length()));
            android.util.Log.d("VlessRepository", "VLESS: Check node \"" + nodeName + "\" ping=" + p + ", available=" + n.proxyInfo.available);

            // Skip only definitely unavailable nodes (ping = 9999 means failed)
            if (p == 9999) {
                android.util.Log.d("VlessRepository", "VLESS: Skip - ping is 9999 (failed)");
                continue;
            }

            // Unknown ping (-1) is treated as potential alternative with moderate priority
            if (p < 0) {
                p = 5000; // Assign moderate ping for unknown nodes
                android.util.Log.d("VlessRepository", "VLESS: Unknown ping, using 5000ms");
            }

            if (p < bestPing) {
                bestPing = p;
                best = n;
                android.util.Log.d("VlessRepository", "VLESS: New best: \"" + nodeName + "\" with ping=" + bestPing);
            }
        }
        
        if (best != null) {
            android.util.Log.i("VlessRepository", "VLESS: Found best alternative: \"" + (best.name != null ? best.name : "node") + "\" ping=" + bestPing);
        } else {
            android.util.Log.w("VlessRepository", "VLESS: No alternatives found!");
        }
        
        return best;
    }

    private void updateNodePing(VlessNode node, boolean viaSocks5, int socks5Port) {
        if (node == null) return;
        // Only signal "checking" — don't reset ping/available to avoid UI flicker between sweeps.
        node.proxyInfo.checking = true;

        VlessUriUtils.HostPort hp = VlessUriUtils.extractHostPort(node.uri);
        if (hp == null) {
            node.proxyInfo.checking = false;
            node.proxyInfo.ping = 9999;
            node.proxyInfo.available = false;
            android.util.Log.d("VlessRepository", "VLESS: ping [" + (node.name != null ? node.name : "unknown") + "] FAILED - invalid URI");
            return;
        }

        long latencyMs;
        if (viaSocks5) {
            // For active node: check BOTH SOCKS proxy AND direct connection to server
            // Server is unavailable if EITHER fails
            long socksLatency = pingHttpViaSocks5(socks5Port);
            long directLatency = pingTcpDirect(hp.host, hp.port, 1500);
            
            // Server is only available if BOTH checks pass
            if (socksLatency < 9999 && directLatency < 9999) {
                latencyMs = socksLatency; // Use SOCKS latency for active connection
                android.util.Log.d("VlessRepository", "VLESS: ping [" + (node.name != null ? node.name : "unknown") + "] SOCKS=" + socksLatency + "ms, direct=" + directLatency + "ms");
            } else {
                latencyMs = 9999;
                android.util.Log.w("VlessRepository", "VLESS: ping [" + (node.name != null ? node.name : "unknown") + "] FAILED - SOCKS=" + socksLatency + "ms, direct=" + directLatency + "ms");
            }
        } else {
            // For inactive nodes: use direct TCP connection to the server
            long start = System.nanoTime();
            boolean ok = false;
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(hp.host, hp.port), 700);
                ok = true;
            } catch (Exception ignored) {
            }
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            latencyMs = ok ? elapsed : 9999;
            android.util.Log.d("VlessRepository", "VLESS: ping [" + (node.name != null ? node.name : "unknown") + "] direct = " + latencyMs + "ms");
        }

        node.proxyInfo.checking = false;
        node.proxyInfo.ping = latencyMs;
        // Mark as available only if ping succeeded (< 9999)
        node.proxyInfo.available = latencyMs < 9999;
        
        android.util.Log.i("VlessRepository", "VLESS: ping [" + (node.name != null ? node.name : "unknown") + "] = " + latencyMs + "ms, available=" + node.proxyInfo.available);
    }

    /**
     * Simple TCP ping to remote host:port
     * Returns latency in ms, or 9999 on failure
     */
    private static long pingTcpDirect(String host, int port, int timeoutMs) {
        long start = System.nanoTime();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            android.util.Log.d("VlessRepository", "VLESS: TCP direct to " + host + ":" + port + " = " + elapsed + "ms");
            return elapsed;
        } catch (Exception e) {
            android.util.Log.d("VlessRepository", "VLESS: TCP direct to " + host + ":" + port + " FAILED - " + e.getMessage());
            return 9999;
        }
    }

    /**
     * Measures HTTP latency through local SOCKS5 proxy to https://www.gstatic.com/generate_204.
     * Returns latency in ms, or 9999 on failure.
     */
    private static long pingHttpViaSocks5(int socks5Port) {
        long start = System.nanoTime();
        HttpURLConnection conn = null;
        try {
            URL url = new URL("https://www.gstatic.com/generate_204");
            conn = (HttpURLConnection) url.openConnection(
                new java.net.Proxy(java.net.Proxy.Type.SOCKS,
                    new InetSocketAddress("127.0.0.1", socks5Port))
            );
            conn.setConnectTimeout(1500);
            conn.setReadTimeout(1500);
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", DEFAULT_USER_AGENT);
            conn.connect();

            int code = conn.getResponseCode();
            long elapsed = (System.nanoTime() - start) / 1_000_000;

            // 204 No Content is expected, but accept 200-299
            if (code >= 200 && code < 300) {
                android.util.Log.d("VlessRepository", "VLESS: gstatic HTTP " + code + " in " + elapsed + "ms");
                return elapsed;
            }
            android.util.Log.d("VlessRepository", "VLESS: gstatic HTTP " + code + " - FAIL");
            return 9999;
        } catch (Exception e) {
            android.util.Log.d("VlessRepository", "VLESS: gstatic exception: " + e.getMessage());
            return 9999;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private void notifyChanged() {
        AndroidUtilities.runOnUIThread(() -> {
            for (Listener l : listeners) {
                try {
                    l.onVlessDataChanged();
                } catch (Throwable ignored) {
                }
            }
        });
    }

    private void loadFromPrefs() {
        String raw = prefs.getString(KEY_DATA_JSON, null);
        if (raw == null || raw.trim().isEmpty()) {
            // defaults
            activeUri = "";
            autoEnableVpn = true;
            autoSwitchVpn = true;
            sortByPing = false;
            return;
        }

        try {
            JSONObject root = new JSONObject(raw);
            activeUri = root.optString("active_uri", "");
            autoEnableVpn = root.optBoolean("auto_enable_vpn", true);
            autoSwitchVpn = root.optBoolean("auto_switch_vpn", true);
            sortByPing = root.optBoolean("sort_by_ping", false);

            localPort = root.optInt("local_port", 20808);

            JSONArray manual = root.optJSONArray("manual");
            manualNodes.clear();
            if (manual != null) {
                for (int i = 0; i < manual.length(); i++) {
                    JSONObject obj = manual.optJSONObject(i);
                    if (obj == null) continue;
                    String uri = obj.optString("uri", "");
                    String name = obj.optString("name", "");
                    if (uri.isEmpty()) continue;
                    manualNodes.add(new VlessNode(uri, name, true, null, localPort));
                }
            }

            JSONArray subs = root.optJSONArray("subs");
            subscriptions.clear();
            if (subs != null) {
                for (int i = 0; i < subs.length(); i++) {
                    JSONObject obj = subs.optJSONObject(i);
                    if (obj == null) continue;
                    String id = obj.optString("id", "");
                    String url = obj.optString("url", "");
                    String name = obj.optString("name", "");
                    JSONArray nodesArr = obj.optJSONArray("nodes");
                    VlessSubscription sub = new VlessSubscription(id, url, name);
                    if (nodesArr != null) {
                        for (int k = 0; k < nodesArr.length(); k++) {
                            JSONObject nObj = nodesArr.optJSONObject(k);
                            if (nObj == null) continue;
                            String uri = nObj.optString("uri", "");
                            String nodeName = nObj.optString("name", "");
                            if (uri.isEmpty()) continue;
                            sub.nodes.add(new VlessNode(uri, nodeName, false, id, localPort));
                        }
                    }
                    subscriptions.add(sub);
                }
            }
        } catch (Exception ignored) {
        }
    }

    private void saveToPrefs() {
        synchronized (lock) {
            saveToPrefsLocked();
        }
    }

    private void saveToPrefsLocked() {
        try {
            JSONObject root = new JSONObject();
            root.put("active_uri", activeUri == null ? "" : activeUri);
            root.put("auto_enable_vpn", autoEnableVpn);
            root.put("auto_switch_vpn", autoSwitchVpn);
            root.put("sort_by_ping", sortByPing);
            root.put("local_port", localPort);

            JSONArray manual = new JSONArray();
            for (VlessNode n : manualNodes) {
                JSONObject o = new JSONObject();
                o.put("uri", n.uri);
                o.put("name", n.name == null ? "" : n.name);
                manual.put(o);
            }
            root.put("manual", manual);

            JSONArray subs = new JSONArray();
            for (VlessSubscription s : subscriptions) {
                JSONObject o = new JSONObject();
                o.put("id", s.id);
                o.put("url", s.url);
                o.put("name", s.name);
                JSONArray nodesArr = new JSONArray();
                for (VlessNode n : s.nodes) {
                    JSONObject no = new JSONObject();
                    no.put("uri", n.uri);
                    no.put("name", n.name == null ? "" : n.name);
                    nodesArr.put(no);
                }
                o.put("nodes", nodesArr);
                subs.put(o);
            }
            root.put("subs", subs);

            prefs.edit().putString(KEY_DATA_JSON, root.toString()).apply();
        } catch (Exception ignored) {
        }
    }

    private List<VlessNode> getAllNodesFlattened() {
        synchronized (lock) {
            return getAllNodesFlattenedLocked();
        }
    }

    private List<VlessNode> getAllNodesFlattenedLocked() {
        List<VlessNode> out = new ArrayList<>(manualNodes.size());
        out.addAll(manualNodes);
        for (VlessSubscription s : subscriptions) {
            out.addAll(s.nodes);
        }
        return out;
    }

    private static String hostFromUrl(String url) {
        try {
            URL u = new URL(url);
            return u.getHost() == null ? "" : u.getHost();
        } catch (Exception ignored) {
            return "";
        }
    }

    private boolean addOrUpdateSubscriptionInternal(String url) {
        String trimmed = url == null ? "" : url.trim();
        if (!(trimmed.startsWith("http://") || trimmed.startsWith("https://"))) {
            return false;
        }

        String content;
        try {
            content = fetchText(trimmed);
        } catch (Exception e) {
            android.util.Log.e("VlessRepository", "VLESS: Failed to fetch subscription: " + e.getMessage());
            return false;
        }

        List<VlessSubscriptionParser.NodeSeed> seeds = VlessSubscriptionParser.extractNodesFromSubscriptionText(content);
        if (seeds.isEmpty()) {
            android.util.Log.w("VlessRepository", "VLESS: No nodes found in subscription");
            return false;
        }

        String id = trimmed;
        String name = hostFromUrl(trimmed);

        int validCount = 0;
        int invalidCount = 0;
        int duplicateCount = 0;

        synchronized (lock) {
            VlessSubscription sub = null;
            for (VlessSubscription s : subscriptions) {
                if (id.equals(s.id)) {
                    sub = s;
                    break;
                }
            }
            if (sub == null) {
                sub = new VlessSubscription(id, trimmed, name);
                subscriptions.add(sub);
            } else {
                sub.url = trimmed;
                sub.name = name;
            }

            sub.nodes.clear();
            for (VlessSubscriptionParser.NodeSeed seed : seeds) {
                if (seed == null || seed.uri == null) continue;
                String normalizedUri = normalizeNodeUri(seed.uri);
                if (normalizedUri.isEmpty()) {
                    invalidCount++;
                    continue;
                }
                // Validate URI before storing it to avoid "click does nothing" later.
                VlessConfigParser.ParsedConfig parsed = VlessConfigParser.parseToLibConfigJson(normalizedUri, localPort);
                if (parsed == null) {
                    android.util.Log.w("VlessRepository", "VLESS: Invalid node URI: " + 
                        (normalizedUri.length() > 50 ? normalizedUri.substring(0, 50) + "..." : normalizedUri));
                    invalidCount++;
                    continue;
                }
                // de-dup by uri across all nodes: keep first occurrence.
                VlessNode existing = findNodeByUri(normalizedUri);
                if (existing != null) {
                    duplicateCount++;
                    continue;
                }
                String nodeName = seed.name != null && !seed.name.trim().isEmpty() ? seed.name : parsed.name;
                sub.nodes.add(new VlessNode(normalizedUri, nodeName, false, id, localPort));
                validCount++;
            }

            saveToPrefsLocked();
        }

        android.util.Log.i("VlessRepository", "VLESS: Subscription updated: " + validCount + " valid, " + 
            invalidCount + " invalid, " + duplicateCount + " duplicates");

        if (validCount == 0) {
            android.util.Log.e("VlessRepository", "VLESS: Subscription added but NO VALID nodes found!");
        }

        return validCount > 0;
    }

    private static String normalizeNodeUri(String uri) {
        if (uri == null) return "";
        String out = uri.trim();
        if (out.isEmpty()) return "";
        int ws = -1;
        for (int i = 0; i < out.length(); i++) {
            if (Character.isWhitespace(out.charAt(i))) {
                ws = i;
                break;
            }
        }
        if (ws > 0) {
            out = out.substring(0, ws);
        }
        return out.trim();
    }

    private String fetchText(String urlStr) throws Exception {
        String ua = USER_AGENTS[new Random().nextInt(USER_AGENTS.length)];

        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setConnectTimeout(12000);
        conn.setReadTimeout(20000);
        conn.setInstanceFollowRedirects(true);
        conn.setRequestProperty("User-Agent", ua);

        int code = conn.getResponseCode();
        if (code < 200 || code >= 300) {
            throw new IllegalStateException("HTTP " + code);
        }

        try (java.io.InputStream in = conn.getInputStream()) {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int read;
            int total = 0;
            final int maxBytes = 1024 * 1024; // 1 MB
            while ((read = in.read(buf)) >= 0) {
                total += read;
                if (total > maxBytes) {
                    break;
                }
                baos.write(buf, 0, read);
            }
            return new String(baos.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private static final class VlessSubscription {
        String id;
        String url;
        String name;
        final List<VlessNode> nodes = new ArrayList<>();

        VlessSubscription(String id, String url, String name) {
            this.id = id;
            this.url = url;
            this.name = name;
        }
    }
}

