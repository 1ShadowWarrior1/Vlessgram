package org.telegram.vless;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Converts VLESS/Trojan/SS URIs into a JSON config for libvless.
 * Ported from the provided Python example ({@code Vless.py}).
 */
public final class VlessConfigParser {
    private VlessConfigParser() {
    }

    public static final class ParsedConfig {
        public final String name;
        public final String configJson;

        private ParsedConfig(String name, String configJson) {
            this.name = name;
            this.configJson = configJson;
        }
    }

    public static ParsedConfig parseToLibConfigJson(String uri, int localPort) {
        try {
            if (uri == null) {
                android.util.Log.d("VlessConfigParser", "VLESS: parseToLibConfigJson - URI is null");
                return null;
            }
            String s = uri.trim();
            if (s.isEmpty()) {
                android.util.Log.d("VlessConfigParser", "VLESS: parseToLibConfigJson - URI is empty");
                return null;
            }
            String low = s.toLowerCase(Locale.ROOT);

            String scheme;
            if (low.startsWith("vless://")) {
                scheme = "vless";
            } else if (low.startsWith("trojan://")) {
                scheme = "trojan";
            } else if (low.startsWith("ss://")) {
                scheme = "ss";
            } else {
                android.util.Log.d("VlessConfigParser", "VLESS: parseToLibConfigJson - Unknown scheme: " + 
                    (s.length() > 20 ? s.substring(0, 20) + "..." : s));
                return null;
            }

            // Fragment part (after '#') is treated as a "name" (if present).
            String name = null;
            String sNoFrag = s;
            int hashIdx = s.indexOf('#');
            if (hashIdx >= 0) {
                sNoFrag = s.substring(0, hashIdx);
                String nameRaw = s.substring(hashIdx + 1);
                name = safeUrlDecode(nameRaw).trim();
                if (name.isEmpty()) {
                    name = null;
                }
            }

            String defaultName;
            if ("vless".equals(scheme)) {
                defaultName = "VLESS Node";
            } else if ("trojan".equals(scheme)) {
                defaultName = "Trojan Node";
            } else {
                defaultName = "Shadowsocks Node";
            }
            if (name == null) {
                name = defaultName;
            }

            Map<String, String> params = new HashMap<>();

            JSONObject root = new JSONObject();
            root.put("log", new JSONObject().put("loglevel", "none"));

            JSONArray inbounds = new JSONArray();
            JSONObject inbound = new JSONObject();
            inbound.put("tag", "socks-in");
            inbound.put("port", localPort);
            inbound.put("listen", "127.0.0.1");
            inbound.put("protocol", "socks");
            inbound.put("settings", new JSONObject().put("auth", "noauth").put("udp", true).put("ip", "127.0.0.1"));
            inbound.put("sniffing", new JSONObject()
                    .put("enabled", true)
                    .put("destOverride", new JSONArray().put("http").put("tls"))
                    .put("routeOnly", false));
            inbounds.put(inbound);
            root.put("inbounds", inbounds);

            JSONArray outbounds = new JSONArray();

            if ("vless".equals(scheme)) {
                String part = sNoFrag.substring("vless://".length());
                int at = part.indexOf('@');
                if (at < 0) {
                    android.util.Log.d("VlessConfigParser", "VLESS: vless - Missing '@' separator");
                    return null;
                }
                String uuidStr = part.substring(0, at);
                String rest = part.substring(at + 1);

                String hostPortPart;
                String paramsRaw = "";
                int qIdx = rest.indexOf('?');
                if (qIdx >= 0) {
                    hostPortPart = rest.substring(0, qIdx);
                    paramsRaw = rest.substring(qIdx + 1);
                } else {
                    hostPortPart = rest;
                }
                params = parseParams(paramsRaw);

                HostPort hp = parseHostPort(hostPortPart);
                if (hp == null) {
                    android.util.Log.d("VlessConfigParser", "VLESS: vless - Invalid host:port: " + hostPortPart);
                    return null;
                }

                String flow = params.getOrDefault("flow", "");
                String net = params.getOrDefault("type", "tcp");
                String security = params.getOrDefault("security", "none");

                JSONObject settings = new JSONObject();
                settings.put("vnext", new JSONArray().put(new JSONObject()
                        .put("address", hp.host)
                        .put("port", hp.port)
                        .put("users", new JSONArray().put(new JSONObject()
                                .put("id", uuidStr)
                                .put("encryption", "none")
                                .put("flow", flow)))));

                JSONObject streamSettings = new JSONObject();
                streamSettings.put("network", net);
                streamSettings.put("security", security);

                String fp = params.getOrDefault("fp", "chrome");
                if ("reality".equals(security)) {
                    streamSettings.put("realitySettings", new JSONObject()
                            .put("show", false)
                            .put("fingerprint", fp)
                            .put("serverName", params.getOrDefault("sni", ""))
                            .put("publicKey", params.getOrDefault("pbk", ""))
                            .put("shortId", params.getOrDefault("sid", ""))
                            .put("spiderX", params.getOrDefault("spx", "")));
                } else if ("tls".equals(security)) {
                    streamSettings.put("tlsSettings", new JSONObject()
                            .put("serverName", params.getOrDefault("sni", ""))
                            .put("allowInsecure", false)
                            .put("fingerprint", fp));
                }

                if ("ws".equals(net)) {
                    streamSettings.put("wsSettings", new JSONObject()
                            .put("path", params.getOrDefault("path", "/"))
                            .put("headers", new JSONObject()
                                    .put("Host", params.getOrDefault("host", params.getOrDefault("sni", "")))));
                } else if ("grpc".equals(net)) {
                    String mode = params.getOrDefault("mode", "multi");
                    streamSettings.put("grpcSettings", new JSONObject()
                            .put("serviceName", params.getOrDefault("serviceName", ""))
                            .put("multiMode", "multi".equals(mode)));
                }

                outbounds.put(new JSONObject()
                        .put("tag", "proxy")
                        .put("protocol", "vless")
                        .put("settings", settings)
                        .put("streamSettings", streamSettings));
            } else if ("trojan".equals(scheme)) {
                String part = sNoFrag.substring("trojan://".length());
                int at = part.indexOf('@');
                if (at < 0) {
                    android.util.Log.d("VlessConfigParser", "VLESS: trojan - Missing '@' separator");
                    return null;
                }
                String password = part.substring(0, at);
                String rest = part.substring(at + 1);

                String hostPortPart;
                String paramsRaw = "";
                int qIdx = rest.indexOf('?');
                if (qIdx >= 0) {
                    hostPortPart = rest.substring(0, qIdx);
                    paramsRaw = rest.substring(qIdx + 1);
                } else {
                    hostPortPart = rest;
                }
                params = parseParams(paramsRaw);

                HostPort hp = parseHostPort(hostPortPart);
                if (hp == null) {
                    android.util.Log.d("VlessConfigParser", "VLESS: trojan - Invalid host:port: " + hostPortPart);
                    return null;
                }

                String net = params.getOrDefault("type", "tcp");
                String security = params.getOrDefault("security", "tls");
                String fp = params.getOrDefault("fp", "chrome");

                JSONObject settings = new JSONObject();
                settings.put("servers", new JSONArray().put(new JSONObject()
                        .put("address", hp.host)
                        .put("port", hp.port)
                        .put("password", password)));

                JSONObject streamSettings = new JSONObject();
                streamSettings.put("network", net);
                streamSettings.put("security", security);

                if ("reality".equals(security)) {
                    streamSettings.put("realitySettings", new JSONObject()
                            .put("show", false)
                            .put("fingerprint", fp)
                            .put("serverName", params.getOrDefault("sni", ""))
                            .put("publicKey", params.getOrDefault("pbk", ""))
                            .put("shortId", params.getOrDefault("sid", ""))
                            .put("spiderX", params.getOrDefault("spx", "")));
                } else if ("tls".equals(security)) {
                    streamSettings.put("tlsSettings", new JSONObject()
                            .put("serverName", params.getOrDefault("sni", ""))
                            .put("allowInsecure", false)
                            .put("fingerprint", fp));
                }

                if ("ws".equals(net)) {
                    streamSettings.put("wsSettings", new JSONObject()
                            .put("path", params.getOrDefault("path", "/"))
                            .put("headers", new JSONObject()
                                    .put("Host", params.getOrDefault("host", params.getOrDefault("sni", "")))));
                } else if ("grpc".equals(net)) {
                    String mode = params.getOrDefault("mode", "multi");
                    streamSettings.put("grpcSettings", new JSONObject()
                            .put("serviceName", params.getOrDefault("serviceName", ""))
                            .put("multiMode", "multi".equals(mode)));
                }

                outbounds.put(new JSONObject()
                        .put("tag", "proxy")
                        .put("protocol", "trojan")
                        .put("settings", settings)
                        .put("streamSettings", streamSettings));
            } else {
                // Shadowsocks: ss://<base64(method:pass)>@host:port#name?params
                String part = sNoFrag.substring("ss://".length());
                String base;
                String paramsRaw = "";
                int qIdx = part.indexOf('?');
                if (qIdx >= 0) {
                    base = part.substring(0, qIdx);
                    paramsRaw = part.substring(qIdx + 1);
                } else {
                    base = part;
                }
                params = parseParams(paramsRaw);

                int at = base.lastIndexOf('@');
                if (at < 0) {
                    return null;
                }
                String userinfoB64 = base.substring(0, at);
                String hostPortPart = base.substring(at + 1);

                HostPort hp = parseHostPort(hostPortPart);
                if (hp == null) {
                    return null;
                }

                String decoded = decodeSsUserInfo(userinfoB64);
                int colon = decoded.indexOf(':');
                if (colon <= 0) {
                    return null;
                }
                String method = decoded.substring(0, colon);
                String password = decoded.substring(colon + 1);

                String net = params.getOrDefault("type", "tcp");
                String security = params.getOrDefault("security", "none");
                String fp = params.getOrDefault("fp", "chrome");

                JSONObject settings = new JSONObject();
                settings.put("servers", new JSONArray().put(new JSONObject()
                        .put("address", hp.host)
                        .put("port", hp.port)
                        .put("method", method)
                        .put("password", password)));

                JSONObject streamSettings = new JSONObject();
                streamSettings.put("network", net);
                streamSettings.put("security", security);

                if ("reality".equals(security)) {
                    streamSettings.put("realitySettings", new JSONObject()
                            .put("show", false)
                            .put("fingerprint", fp)
                            .put("serverName", params.getOrDefault("sni", ""))
                            .put("publicKey", params.getOrDefault("pbk", ""))
                            .put("shortId", params.getOrDefault("sid", ""))
                            .put("spiderX", params.getOrDefault("spx", "")));
                } else if ("tls".equals(security)) {
                    streamSettings.put("tlsSettings", new JSONObject()
                            .put("serverName", params.getOrDefault("sni", ""))
                            .put("allowInsecure", false)
                            .put("fingerprint", fp));
                }

                if ("ws".equals(net)) {
                    streamSettings.put("wsSettings", new JSONObject()
                            .put("path", params.getOrDefault("path", "/"))
                            .put("headers", new JSONObject()
                                    .put("Host", params.getOrDefault("host", params.getOrDefault("sni", "")))));
                } else if ("grpc".equals(net)) {
                    String mode = params.getOrDefault("mode", "multi");
                    streamSettings.put("grpcSettings", new JSONObject()
                            .put("serviceName", params.getOrDefault("serviceName", ""))
                            .put("multiMode", "multi".equals(mode)));
                }

                outbounds.put(new JSONObject()
                        .put("tag", "proxy")
                        .put("protocol", "shadowsocks")
                        .put("settings", settings)
                        .put("streamSettings", streamSettings));
            }

            root.put("outbounds", outbounds);
            return new ParsedConfig(name, root.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private static String decodeSsUserInfo(String sB64) {
        try {
            if (sB64 == null) {
                return "";
            }
            String raw = sB64.trim();
            if (raw.isEmpty()) {
                return "";
            }

            raw = raw.replace('-', '+').replace('_', '/');
            int missingPadding = raw.length() % 4;
            if (missingPadding != 0) {
                int pads = 4 - missingPadding;
                StringBuilder sb = new StringBuilder(raw);
                for (int i = 0; i < pads; i++) {
                    sb.append('=');
                }
                raw = sb.toString();
            }

            byte[] decoded = Base64.getDecoder().decode(raw);
            return new String(decoded, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "";
        }
    }

    private static Map<String, String> parseParams(String paramsRaw) {
        Map<String, String> params = new HashMap<>();
        if (paramsRaw == null || paramsRaw.isEmpty()) {
            return params;
        }
        for (String pair : paramsRaw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String k = pair.substring(0, eq);
            String v = pair.substring(eq + 1);
            params.put(k, safeUrlDecode(v));
        }
        return params;
    }

    private static HostPort parseHostPort(String hostPort) {
        if (hostPort == null) {
            return null;
        }
        String hp = hostPort.trim();
        if (hp.isEmpty()) {
            return null;
        }
        try {
            if (hp.startsWith("[")) {
                int end = hp.indexOf(']');
                if (end < 0) {
                    return null;
                }
                if (hp.length() <= end + 2 || hp.charAt(end + 1) != ':') {
                    return null;
                }
                String host = hp.substring(1, end);
                String portStr = hp.substring(end + 2);
                int port = Integer.parseInt(portStr);
                return new HostPort(host, port);
            } else {
                int colon = hp.lastIndexOf(':');
                if (colon < 0) {
                    return null;
                }
                String host = hp.substring(0, colon);
                String portStr = hp.substring(colon + 1);
                int port = Integer.parseInt(portStr);
                return new HostPort(host, port);
            }
        } catch (Exception e) {
            return null;
        }
    }

    private static String safeUrlDecode(String s) {
        if (s == null) {
            return "";
        }
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            return s;
        }
    }

    private static final class HostPort {
        final String host;
        final int port;

        private HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }
}

