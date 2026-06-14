package nexus;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Dashboard {
    private final Map<Integer, RaftNodeImpl> nodes;
    private final LoadBalancer               lb;
    private final GossipProtocol             gossip;
    private final Bulkhead                   chatBulkhead;
    private final Bulkhead                   kvBulkhead;
    private final MetricsCollector           metrics;

    public Dashboard(Map<Integer, RaftNodeImpl> nodes, LoadBalancer lb,
                     GossipProtocol gossip, Bulkhead chatBulkhead, Bulkhead kvBulkhead,
                     MetricsCollector metrics) {
        this.nodes=nodes; this.lb=lb; this.gossip=gossip;
        this.chatBulkhead=chatBulkhead; this.kvBulkhead=kvBulkhead; this.metrics=metrics;
    }
    public Dashboard(Map<Integer, RaftNodeImpl> nodes, LoadBalancer lb,
                     GossipProtocol gossip, Bulkhead chatBulkhead, Bulkhead kvBulkhead) {
        this(nodes,lb,gossip,chatBulkhead,kvBulkhead,null);
    }
    public Dashboard(Map<Integer, RaftNodeImpl> nodes, LoadBalancer lb) {
        this(nodes,lb,null,null,null,null);
    }

    /** يسجّل مسارات لوحة التحكم على خادم HTTP مُمرَّر (بورت واحد للكل) */
    public void registerOn(HttpServer server) {
        server.createContext("/",        ex -> respond(ex,"text/html; charset=utf-8",       buildPage()));
        server.createContext("/status",  ex -> respond(ex,"application/json; charset=utf-8",statusJson()));
        server.createContext("/metrics", ex -> respond(ex,"application/json; charset=utf-8",metricsJson()));
        server.createContext("/action",  ex -> { handleAction(ex.getRequestURI()); respond(ex,"application/json","{\"ok\":true}"); });
    }

    public void start(int port) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        registerOn(server);
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        System.out.println("Dashboard: http://localhost:" + port);
    }

    private void handleAction(URI uri) {
        Map<String,String> q = parseQuery(uri.getRawQuery());
        String cmd = q.get("cmd"); if(cmd==null) return;
        try {
            switch(cmd) {
                case "kill"      -> nodes.get(Integer.parseInt(q.get("id"))).crash();
                case "revive"    -> nodes.get(Integer.parseInt(q.get("id"))).revive();
                case "put"       -> lb.write(java.util.UUID.randomUUID().toString().substring(0,8),"put "+q.get("k")+" "+q.get("v"));
                case "loadtest"  -> lb.loadTest(50,"color");
                case "partition" -> partitionLeader();
                case "heal"      -> { for(RaftNodeImpl n:nodes.values()) n.unblockAll(); }
                case "algo"      -> lb.setAlgorithm(LoadBalancer.Algorithm.valueOf(q.get("name")));
                case "weight"    -> lb.setWeight(Integer.parseInt(q.get("id")),Integer.parseInt(q.get("w")));
                case "fail"      -> nodes.get(Integer.parseInt(q.get("id"))).setFailing(true);
                case "recover"   -> nodes.get(Integer.parseInt(q.get("id"))).setFailing(false);
            }
        } catch(Exception ignored){}
    }

    private void partitionLeader() {
        Integer lid=null;
        for(Map.Entry<Integer,RaftNodeImpl> e:nodes.entrySet()) if(e.getValue().isLeader()){lid=e.getKey();break;}
        if(lid==null) lid=nodes.keySet().iterator().next();
        int lp=1098+lid;
        for(Map.Entry<Integer,RaftNodeImpl> e:nodes.entrySet()) {
            if(e.getKey().equals(lid)) { for(Integer o:nodes.keySet()) if(!o.equals(lid)) e.getValue().blockPort(1098+o); }
            else e.getValue().blockPort(lp);
        }
    }

    private String metricsJson() {
        return metrics!=null?metrics.snapshotJson():"{\"rps\":0,\"p50\":0,\"p95\":0,\"p99\":0,\"avg\":\"0.0\",\"total\":0,\"history\":[]}";
    }

    private String statusJson() {
        Map<Integer,Integer> served=lb.servedSnapshot(); Map<Integer,Double> avgR=lb.avgResponseSnapshot();
        Map<Integer,Integer> wts=lb.weightsSnapshot();   Map<Integer,String> cbs=lb.circuitSnapshot();
        StringBuilder sb=new StringBuilder("{\"nodes\":["); boolean first=true;
        for(Map.Entry<Integer,RaftNodeImpl> e:nodes.entrySet()) {
            if(!first)sb.append(","); first=false;
            RaftNodeImpl n=e.getValue(); int id=e.getKey();
            String gs=gossip!=null?gossip.getStatus(id).name():"UNKNOWN";
            sb.append("{\"id\":").append(id).append(",\"role\":\"").append(n.getRoleLabel()).append("\"")
              .append(",\"term\":").append(n.getTerm()).append(",\"log\":").append(n.getLogSize())
              .append(",\"committed\":").append(n.getCommitted()).append(",\"alive\":").append(n.isAlive())
              .append(",\"isolated\":").append(n.isIsolated()).append(",\"served\":").append(served.getOrDefault(id,0))
              .append(",\"avgMs\":\"").append(String.format("%.1f",avgR.getOrDefault(id,0.0))).append("\"")
              .append(",\"weight\":").append(wts.getOrDefault(id,1))
              .append(",\"circuit\":\"").append(cbs.getOrDefault(id,"CLOSED")).append("\"")
              .append(",\"gossip\":\"").append(gs).append("\"")
              .append(",\"kv\":").append(kvJson(n.snapshotKv())).append("}");
        }
        sb.append("],\"algo\":\"").append(lb.getAlgorithm().name()).append("\"");
        sb.append(",\"bulkheads\":{\"chat\":{\"active\":").append(chatBulkhead!=null?chatBulkhead.getActive():0)
          .append(",\"rejected\":").append(chatBulkhead!=null?chatBulkhead.getRejected():0).append("}")
          .append(",\"kv\":{\"active\":").append(kvBulkhead!=null?kvBulkhead.getActive():0)
          .append(",\"rejected\":").append(kvBulkhead!=null?kvBulkhead.getRejected():0).append("}}");
        sb.append(",\"gossipLogs\":[");
        if(gossip!=null){List<String> logs=gossip.getRecentLogs(6);for(int i=0;i<logs.size();i++){if(i>0)sb.append(",");sb.append("\"").append(esc(logs.get(i))).append("\"");}}
        return sb.append("]}").toString();
    }

    private String kvJson(Map<String,String> kv) {
        StringBuilder sb=new StringBuilder("{"); boolean f=true;
        for(Map.Entry<String,String> e:kv.entrySet()){if(!f)sb.append(",");f=false;sb.append("\"").append(esc(e.getKey())).append("\":\"").append(esc(e.getValue())).append("\"");}
        return sb.append("}").toString();
    }
    private Map<String,String> parseQuery(String raw){
        Map<String,String> m=new HashMap<>();if(raw==null)return m;
        for(String p:raw.split("&")){int i=p.indexOf('=');if(i>0)m.put(dec(p.substring(0,i)),dec(p.substring(i+1)));}
        return m;
    }
    private String dec(String s){return URLDecoder.decode(s,StandardCharsets.UTF_8);}
    private String esc(String s){return s==null?"":s.replace("\\","\\\\").replace("\"","\\\"").replace("\n"," ");}
    private void respond(HttpExchange ex,String type,String body) throws IOException {
        byte[] b=body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type",type);
        ex.getResponseHeaders().add("Access-Control-Allow-Origin","*");
        ex.sendResponseHeaders(200,b.length);
        try(OutputStream os=ex.getResponseBody()){os.write(b);}
    }
    // يحمّل الواجهة من ملف خارجي (dashboard.html) ويعود للنسخة المضمّنة عند الفشل
    private volatile String cachedPage = null;
    private String buildPage(){
        if (cachedPage != null) return cachedPage;
        // 1) من الـ classpath (يعمل داخل الحاوية والـ jar)
        for (String res : new String[]{"/dashboard.html", "/nexus/dashboard.html"}) {
            try (java.io.InputStream in = getClass().getResourceAsStream(res)) {
                if (in != null) return cachedPage = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            } catch (Exception ignored) {}
        }
        // 2) من نظام الملفات (تطوير محلي)
        for (String p : new String[]{"dashboard.html", "src/nexus/dashboard.html", "web/dashboard.html"}) {
            try { java.io.File f = new java.io.File(p);
                if (f.exists()) return cachedPage = new String(java.nio.file.Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            } catch (Exception ignored) {}
        }
        // 3) النسخة المضمّنة (احتياطي)
        return PAGE;
    }
    private static final String PAGE =
        "<!DOCTYPE html>\n" +
        "<html lang=\"ar\" dir=\"rtl\">\n" +
        "<head><meta charset=\"utf-8\"><title>NexusChat Mission Control</title>\n" +
        "<style>\n" +
        "*{box-sizing:border-box;margin:0;padding:0}\n" +
        "body{background:#0e1117;color:#e6e6e6;font-family:system-ui,sans-serif;padding:20px}\n" +
        "h1{font-size:19px;font-weight:700;display:flex;align-items:center;gap:10px;margin-bottom:14px}\n" +
        ".pulse{width:10px;height:10px;border-radius:50%;background:#3fb950;animation:pulse 1.5s infinite}\n" +
        "@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}\n" +
        ".tabs{display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid #30363d}\n" +
        ".tab{padding:9px 18px;font-size:13px;cursor:pointer;color:#8b949e;border-bottom:2px solid transparent;margin-bottom:-1px;border-radius:6px 6px 0 0}\n" +
        ".tab:hover{color:#e6e6e6;background:#161b22}.tab.active{color:#58a6ff;border-bottom-color:#58a6ff;background:#161b22}\n" +
        ".tab-content{display:none}.tab-content.active{display:block}\n" +
        ".nodes{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px}\n" +
        ".card{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:14px}\n" +
        ".card.leader{border:2px solid #58a6ff}.card.down{opacity:.45}.card.iso{border:2px dashed #d29922}\n" +
        ".card h2{font-size:14px;font-weight:600;display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:4px}\n" +
        ".badge{font-size:10px;padding:2px 9px;border-radius:8px;font-weight:600}\n" +
        ".b-leader{background:#1f6feb33;color:#58a6ff}.b-follower{background:#8b949e22;color:#8b949e}\n" +
        ".b-down{background:#f8514922;color:#f85149}.b-alive{background:#3fb95022;color:#3fb950}\n" +
        ".b-suspect{background:#d2992233;color:#d29922}.b-dead{background:#f8514922;color:#f85149}\n" +
        ".row{display:flex;justify-content:space-between;font-size:12px;color:#8b949e;padding:2px 0}\n" +
        ".row b{color:#e6e6e6;font-weight:500}\n" +
        ".grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px}\n" +
        ".grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px}\n" +
        ".panel{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:14px}\n" +
        ".panel h3{font-size:12px;color:#8b949e;font-weight:500;margin-bottom:10px;text-transform:uppercase;letter-spacing:.5px}\n" +
        ".algo-grid{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px}\n" +
        ".algo-btn{background:#21262d;border:1px solid #30363d;border-radius:8px;padding:6px 12px;font-size:12px;cursor:pointer;color:#e6e6e6}\n" +
        ".algo-btn:hover{background:#30363d}.algo-btn.active{background:#1f6feb33;border-color:#58a6ff;color:#58a6ff;font-weight:600}\n" +
        ".bar{display:flex;align-items:center;gap:8px;font-size:12px;margin-bottom:7px}\n" +
        ".bar .lbl{width:50px;flex-shrink:0}.track{flex:1;height:12px;background:#0e1117;border-radius:3px;overflow:hidden}\n" +
        ".fill{height:12px;border-radius:3px;transition:width .4s}.fill-blue{background:#58a6ff}.fill-green{background:#3fb950}\n" +
        ".kv{font-family:ui-monospace,monospace;font-size:12px;line-height:1.8}\n" +
        ".feed{display:flex;flex-direction:column;gap:6px;max-height:220px;overflow:auto}\n" +
        ".evt{background:#0e1117;border:1px solid #30363d;border-radius:8px;padding:8px 10px;display:flex;justify-content:space-between;align-items:center;gap:8px}\n" +
        ".evt-msg{font-size:12px;flex:1}.evt-tag{font-size:10px;color:#58a6ff;background:#1f6feb22;padding:2px 8px;border-radius:6px;white-space:nowrap}\n" +
        ".bh-row{display:flex;align-items:center;justify-content:space-between;font-size:12px;background:#0e1117;border-radius:8px;padding:8px 10px;margin-bottom:6px}\n" +
        ".bh-name{color:#8b949e;width:80px}.gossip-log{font-size:11px;font-family:ui-monospace,monospace;color:#8b949e;line-height:1.7;max-height:120px;overflow:auto}\n" +
        ".controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-top:10px}\n" +
        "button{background:#21262d;color:#e6e6e6;border:1px solid #30363d;border-radius:8px;padding:7px 13px;font-size:12px;cursor:pointer;font-family:inherit}\n" +
        "button:hover{background:#30363d}button.danger{color:#f85149;border-color:#f8514944}button.warn{color:#d29922}button.success{color:#3fb950}\n" +
        "button:disabled{opacity:.5;cursor:default}\n" +
        "input{background:#0e1117;color:#e6e6e6;border:1px solid #30363d;border-radius:8px;padding:7px 9px;font-size:12px;width:80px}\n" +
        ".chat-layout{display:grid;grid-template-columns:180px 1fr;gap:16px;height:520px}\n" +
        ".chat-users{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:12px;overflow-y:auto}\n" +
        ".chat-users h3{font-size:11px;color:#8b949e;margin-bottom:8px;text-transform:uppercase}\n" +
        ".user-item{display:flex;align-items:center;gap:7px;padding:6px 8px;border-radius:8px;font-size:13px;cursor:pointer;margin-bottom:3px}\n" +
        ".user-item:hover{background:#21262d}.user-item.selected{background:#1f6feb22;color:#58a6ff}\n" +
        ".dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}.dot.on{background:#3fb950}.dot.off{background:#8b949e}\n" +
        ".offline-badge{font-size:10px;background:#f8514922;color:#f85149;padding:1px 6px;border-radius:8px;margin-right:auto}\n" +
        ".chat-main{display:flex;flex-direction:column;gap:10px}\n" +
        ".chat-header{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:10px 14px;display:flex;align-items:center;justify-content:space-between}\n" +
        ".chat-msgs{flex:1;background:#161b22;border:1px solid #30363d;border-radius:12px;padding:14px;overflow-y:auto;display:flex;flex-direction:column;gap:8px}\n" +
        ".msg{max-width:75%;padding:9px 13px;border-radius:12px;font-size:13px;line-height:1.5}\n" +
        ".msg.sent{background:#1f6feb33;border:1px solid #1f6feb55;align-self:flex-start}\n" +
        ".msg.recv{background:#21262d;border:1px solid #30363d;align-self:flex-end}\n" +
        ".msg-meta{font-size:10px;color:#8b949e;margin-top:4px;display:flex;gap:8px;align-items:center}\n" +
        ".vc-tag{font-family:ui-monospace,monospace;font-size:10px;color:#3fb95088;background:#3fb95011;padding:1px 5px;border-radius:4px}\n" +
        ".status-tag{color:#58a6ff}.status-read{color:#3fb950}\n" +
        ".chat-input-row{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:10px 14px;display:flex;gap:8px;align-items:center}\n" +
        ".chat-input-row input{flex:1;width:auto}\n" +
        ".stats-row{display:flex;gap:10px;flex-wrap:wrap;font-size:12px;color:#8b949e;margin-bottom:12px}\n" +
        ".stat-chip{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:5px 12px}\n" +
        ".stat-chip b{color:#e6e6e6}\n" +
        ".fanout-toggle{display:flex;gap:6px}\n" +
        ".fanout-btn{font-size:11px;padding:4px 10px;border-radius:6px}\n" +
        ".fanout-btn.active{background:#1f6feb33;border-color:#58a6ff;color:#58a6ff}\n" +
        ".raft-wrap{display:grid;grid-template-columns:1fr 280px;gap:16px}\n" +
        ".raft-info-bar{font-size:13px;padding:10px 14px;background:#161b22;border:1px solid #30363d;border-radius:8px;margin-bottom:12px}\n" +
        ".raft-log-row{display:flex;justify-content:space-between;font-size:12px;padding:6px 10px;border-radius:6px;background:#0e1117;margin-bottom:4px}\n" +
        ".raft-legend{display:flex;gap:16px;font-size:12px;margin-top:10px;padding:6px}\n" +
        ".hb-line{stroke-dasharray:8 4;animation:dash-anim 0.5s linear infinite}\n" +
        "@keyframes dash-anim{to{stroke-dashoffset:-24}}\n" +
        ".m-chip{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:6px 14px;font-size:12px}\n" +
        ".m-chip b{color:#e6e6e6}\n" +
        ".metrics-chips{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}\n" +
        ".metrics-legend{display:flex;gap:20px;font-size:12px;margin-top:8px;padding:4px 8px}\n" +
        ".canvas-bg{background:#0d1117;border-radius:8px;padding:8px;border:1px solid #21262d}\n" +
        "#pacelc-result{margin-top:8px}\n" +
        "</style></head><body>\n" +
        "<h1><span class=\"pulse\"></span>NexusChat Mission Control <span style=\"font-size:12px;color:#8b949e;margin-right:auto\" id=\"algo-label\"></span></h1>\n" +
        "<div class=\"tabs\">\n" +
        "  <div class=\"tab active\" onclick=\"switchTab('cluster')\">&#x1F5A5; \u0627\u0644\u0643\u0644\u0633\u062a\u0631</div>\n" +
        "  <div class=\"tab\" onclick=\"switchTab('lb')\">&#x2696; Load Balancer</div>\n" +
        "  <div class=\"tab\" onclick=\"switchTab('chat')\">&#x1F4AC; \u0627\u0644\u062f\u0631\u062f\u0634\u0629</div>\n" +
        "  <div class=\"tab\" onclick=\"switchTab('raft')\">&#x1F3AF; Raft Live</div>\n" +
        "  <div class=\"tab\" onclick=\"switchTab('metrics')\">&#x1F4CA; Metrics</div>\n" +
        "</div>\n" +
        "<div id=\"tab-cluster\" class=\"tab-content active\">\n" +
        "<div class=\"nodes\" id=\"nodes\"></div>\n" +
        "<div class=\"grid3\">\n" +
        "  <div class=\"panel\"><h3>KV Store</h3><div class=\"kv\" id=\"kv\"></div>\n" +
        "    <div class=\"controls\"><input id=\"k\" placeholder=\"\u0645\u0641\u062a\u0627\u062d\"><input id=\"v\" placeholder=\"\u0642\u064a\u0645\u0629\"><button onclick=\"doPut()\" class=\"success\">Put</button></div></div>\n" +
        "  <div class=\"panel\"><h3>Bulkhead</h3><div id=\"bulkheads\"></div></div>\n" +
        "  <div class=\"panel\"><h3>Gossip Protocol</h3><div id=\"gossip-members\" style=\"margin-bottom:8px\"></div><div class=\"gossip-log\" id=\"gossip-log\"></div></div>\n" +
        "</div>\n" +
        "<div class=\"grid2\">\n" +
        "  <div class=\"panel\"><h3>\u0633\u062c\u0644 \u0627\u0644\u0627\u062d\u062f\u0627\u062b</h3><div class=\"feed\" id=\"feed\"></div></div>\n" +
        "  <div class=\"panel\"><h3>\u0627\u0644\u062a\u062d\u0643\u0645</h3>\n" +
        "    <div class=\"controls\">\n" +
        "      <button class=\"warn\" onclick=\"act('partition')\">&#x26A1; \u0627\u0639\u0632\u0644 \u0627\u0644\u0642\u0627\u0626\u062f</button>\n" +
        "      <button onclick=\"act('heal')\">&#x1F527; \u0627\u0635\u0644\u062d \u0627\u0644\u0634\u0628\u0643\u0629</button>\n" +
        "      <button id=\"chaos-btn\" onclick=\"toggleChaos()\">&#x1F534; Chaos</button>\n" +
        "      <button id=\"pacelc-btn\" onclick=\"runPACELC()\">&#x1F4CB; PACELC Proof</button>\n" +
        "    </div>\n" +
        "    <div id=\"pacelc-result\"></div>\n" +
        "  </div>\n" +
        "</div>\n" +
        "</div>\n" +
        "<div id=\"tab-lb\" class=\"tab-content\">\n" +
        "<div class=\"panel\" style=\"margin-bottom:16px\">\n" +
        "  <h3>Load Balancing: <span id=\"algo-label2\" style=\"color:#58a6ff\"></span></h3>\n" +
        "  <div class=\"algo-grid\">\n" +
        "    <button class=\"algo-btn\" onclick=\"setAlgo('ROUND_ROBIN')\">1. Round-Robin</button>\n" +
        "    <button class=\"algo-btn\" onclick=\"setAlgo('WEIGHTED_ROUND_ROBIN')\">2. Weighted RR</button>\n" +
        "    <button class=\"algo-btn\" onclick=\"setAlgo('LEAST_CONNECTIONS')\">3. Least Connections</button>\n" +
        "    <button class=\"algo-btn\" onclick=\"setAlgo('LEAST_RESPONSE_TIME')\">4. Least Resp.Time</button>\n" +
        "    <button class=\"algo-btn\" onclick=\"setAlgo('STICKY_SESSION')\">5. Sticky Session</button>\n" +
        "    <button class=\"algo-btn\" onclick=\"setAlgo('CONSISTENT_HASH')\">6. Consistent Hash</button>\n" +
        "    <button class=\"algo-btn\" onclick=\"setAlgo('POWER_OF_TWO')\">7. Power of Two</button>\n" +
        "    <button class=\"algo-btn\" onclick=\"setAlgo('JOIN_IDLE_QUEUE')\">8. Join-Idle-Queue</button>\n" +
        "  </div>\n" +
        "  <div id=\"lb-bars\"></div>\n" +
        "  <div class=\"controls\"><button onclick=\"act('loadtest')\">&#x1F680; 50 \u0637\u0644\u0628</button>\n" +
        "  <input id=\"wid\" placeholder=\"Node\" style=\"width:60px\"><input id=\"wval\" placeholder=\"\u0648\u0632\u0646\" style=\"width:60px\">\n" +
        "  <button onclick=\"setWeight()\">\u0636\u0628\u0637 \u0648\u0632\u0646</button></div>\n" +
        "</div>\n" +
        "</div>\n" +
        "<div id=\"tab-chat\" class=\"tab-content\">\n" +
        "  <div class=\"stats-row\" id=\"chat-stats\"></div>\n" +
        "  <div class=\"chat-layout\">\n" +
        "    <div class=\"chat-users\">\n" +
        "      <h3>\u0627\u0644\u0645\u0633\u062a\u062e\u062f\u0645\u0648\u0646</h3>\n" +
        "      <div id=\"user-list\"></div>\n" +
        "      <div style=\"margin-top:12px;border-top:1px solid #30363d;padding-top:10px\">\n" +
        "        <div style=\"font-size:11px;color:#8b949e;margin-bottom:6px\">\u0627\u062a\u0635\u0644 \u0643\u0640:</div>\n" +
        "        <input id=\"my-user\" placeholder=\"\u0627\u0633\u0645\u0643\" style=\"width:100%;margin-bottom:6px\">\n" +
        "        <button onclick=\"connectUser()\" style=\"width:100%\" class=\"success\">\u0627\u062a\u0635\u0644</button>\n" +
        "      </div>\n" +
        "    </div>\n" +
        "    <div class=\"chat-main\">\n" +
        "      <div class=\"chat-header\">\n" +
        "        <span id=\"chat-to-label\" style=\"font-size:14px;font-weight:600\">\u0627\u062e\u062a\u0631 \u0645\u0633\u062a\u062e\u062f\u0645</span>\n" +
        "        <div class=\"fanout-toggle\">\n" +
        "          <span style=\"font-size:12px;color:#8b949e;align-self:center\">Fan-out:</span>\n" +
        "          <button class=\"fanout-btn active\" id=\"fo-write\" onclick=\"setFanout('WRITE_TIME')\">Write-Time</button>\n" +
        "          <button class=\"fanout-btn\" id=\"fo-read\" onclick=\"setFanout('READ_TIME')\">Read-Time</button>\n" +
        "        </div>\n" +
        "      </div>\n" +
        "      <div class=\"chat-msgs\" id=\"chat-msgs\"><div style=\"color:#8b949e;font-size:13px;text-align:center;margin-top:40px\">\u0627\u062a\u0635\u0644 \u0623\u0648\u0644\u0627\u064b \u062b\u0645 \u0627\u062e\u062a\u0631 \u0645\u0633\u062a\u062e\u062f\u0645\u0627\u064b</div></div>\n" +
        "      <div class=\"chat-input-row\">\n" +
        "        <input id=\"msg-input\" placeholder=\"\u0627\u0643\u062a\u0628 \u0631\u0633\u0627\u0644\u0629...\" onkeydown=\"if(event.key==='Enter')sendMsg()\">\n" +
        "        <button onclick=\"sendMsg()\" class=\"success\">\u0625\u0631\u0633\u0627\u0644</button>\n" +
        "        <button onclick=\"markRead()\" style=\"font-size:11px\">&#x2713;&#x2713; \u0642\u064f\u0631\u0626</button>\n" +
        "      </div>\n" +
        "    </div>\n" +
        "  </div>\n" +
        "</div>\n" +
        "<div id=\"tab-raft\" class=\"tab-content\">\n" +
        "  <div class=\"raft-info-bar\" id=\"raft-info\">\u062c\u0627\u0631\u064d \u0627\u0644\u062a\u062d\u0645\u064a\u0644...</div>\n" +
        "  <div class=\"raft-wrap\">\n" +
        "    <div>\n" +
        "      <svg id=\"raft-svg\" viewBox=\"0 0 500 460\" style=\"width:100%;height:460px;background:#0d1117;border-radius:12px;border:1px solid #21262d\"></svg>\n" +
        "      <div class=\"raft-legend\">\n" +
        "        <span style=\"color:#58a6ff\">&#x25CE; Leader (heartbeats)</span>\n" +
        "        <span style=\"color:#8b949e\">&#x25CE; Follower</span>\n" +
        "        <span style=\"color:#d29922\">&#x25CE; Candidate</span>\n" +
        "        <span style=\"color:#f85149\">&#x2715; Down</span>\n" +
        "      </div>\n" +
        "    </div>\n" +
        "    <div class=\"panel\">\n" +
        "      <h3>\u062d\u0627\u0644\u0629 \u0627\u0644\u0639\u0642\u062f</h3>\n" +
        "      <div id=\"raft-log\" style=\"margin-bottom:14px\"></div>\n" +
        "      <h3>\u0625\u062c\u0631\u0627\u0621\u0627\u062a</h3>\n" +
        "      <div class=\"controls\" style=\"flex-direction:column;align-items:stretch;gap:6px;margin-top:8px\">\n" +
        "        <button class=\"danger\" onclick=\"act('kill',1)\">&#x26A1; \u0627\u0642\u062a\u0644 Node 1</button>\n" +
        "        <button class=\"danger\" onclick=\"act('kill',2)\">&#x26A1; \u0627\u0642\u062a\u0644 Node 2</button>\n" +
        "        <button class=\"danger\" onclick=\"act('kill',3)\">&#x26A1; \u0627\u0642\u062a\u0644 Node 3</button>\n" +
        "        <button class=\"success\" onclick=\"[1,2,3].forEach(i=>act('revive',i))\">&#x2713; \u0623\u062d\u064a\u0650 \u0627\u0644\u0643\u0644</button>\n" +
        "        <button class=\"warn\" onclick=\"act('partition')\">&#x26A1; Partition \u0627\u0644\u0642\u0627\u0626\u062f</button>\n" +
        "        <button onclick=\"act('heal')\">&#x1F527; Heal \u0627\u0644\u0634\u0628\u0643\u0629</button>\n" +
        "      </div>\n" +
        "    </div>\n" +
        "  </div>\n" +
        "</div>\n" +
        "<div id=\"tab-metrics\" class=\"tab-content\">\n" +
        "  <div class=\"metrics-chips\" id=\"metric-chips\"></div>\n" +
        "  <div class=\"canvas-bg\">\n" +
        "    <canvas id=\"metrics-canvas\" height=\"180\" style=\"width:100%;display:block\"></canvas>\n" +
        "  </div>\n" +
        "  <div class=\"metrics-legend\">\n" +
        "    <span style=\"color:#58a6ff\">&#x2015; RPS (\u0637\u0644\u0628/\u062b\u0627\u0646\u064a\u0629)</span>\n" +
        "    <span style=\"color:#3fb950\">&#x2015; Avg Latency (ms)</span>\n" +
        "  </div>\n" +
        "  <div style=\"margin-top:16px\" class=\"panel\">\n" +
        "    <h3>\u062a\u0648\u0644\u064a\u062f \u062d\u0645\u0644</h3>\n" +
        "    <div class=\"controls\">\n" +
        "      <button onclick=\"act('loadtest')\">&#x1F680; 50 \u0637\u0644\u0628</button>\n" +
        "      <button onclick=\"burstLoad()\">&#x26A1; 250 \u0637\u0644\u0628 (burst)</button>\n" +
        "    </div>\n" +
        "  </div>\n" +
        "</div>\n" +
        "<script>\n" +
        "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550 TABS \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n" +
        "function switchTab(name){\n" +
        "  document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active'));\n" +
        "  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));\n" +
        "  document.getElementById('tab-'+name).classList.add('active');\n" +
        "  const idx={cluster:0,lb:1,chat:2,raft:3,metrics:4}[name];\n" +
        "  document.querySelectorAll('.tab')[idx].classList.add('active');\n" +
        "}\n" +
        "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550 CLUSTER \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n" +
        "let prev=null,events=[];\n" +
        "const ALGO={ROUND_ROBIN:'Round-Robin',WEIGHTED_ROUND_ROBIN:'Weighted RR',LEAST_CONNECTIONS:'Least Connections',LEAST_RESPONSE_TIME:'Least Resp.Time',STICKY_SESSION:'Sticky Session',CONSISTENT_HASH:'Consistent Hash',POWER_OF_TWO:'Power of Two',JOIN_IDLE_QUEUE:'Join-Idle-Queue'};\n" +
        "function pushEvent(msg,tag){events.unshift({msg,tag,t:new Date().toLocaleTimeString()});if(events.length>14)events.pop();renderFeed();}\n" +
        "function renderFeed(){\n" +
        "  const el=document.getElementById('feed');\n" +
        "  el.innerHTML=events.length?events.map(e=>'<div class=\"evt\"><div class=\"evt-msg\">'+e.t+' \u2014 '+e.msg+'</div><span class=\"evt-tag\">'+e.tag+'</span></div>').join(''):'<span style=\"color:#8b949e;font-size:12px\">\u0627\u0643\u0633\u0631 \u0634\u064a\u0621 \u0644\u064a\u0638\u0647\u0631 \u0627\u0644\u0633\u062c\u0644</span>';\n" +
        "}\n" +
        "function topLeader(arr){const ls=arr.filter(n=>n.role==='LEADER');return ls.length?ls.sort((a,b)=>b.term-a.term)[0]:null;}\n" +
        "function detect(d){\n" +
        "  if(!prev){prev=d;return;}\n" +
        "  const ln=topLeader(d.nodes),lb=topLeader(prev.nodes);\n" +
        "  if(ln&&(!lb||lb.id!==ln.id))pushEvent('\u0627\u0646\u062a\u062e\u0627\u0628: Node'+ln.id+' \u0642\u0627\u0626\u062f (term '+ln.term+')','Leader Election');\n" +
        "  d.nodes.forEach(n=>{\n" +
        "    const p=prev.nodes.find(x=>x.id===n.id);if(!p)return;\n" +
        "    if(p.alive&&!n.alive)pushEvent('Node'+n.id+' \u0633\u0642\u0637','Fault Tolerance');\n" +
        "    if(!p.alive&&n.alive)pushEvent('Node'+n.id+' \u0639\u0627\u062f','Self-Healing');\n" +
        "    if(p.circuit==='CLOSED'&&n.circuit==='OPEN')pushEvent('Node'+n.id+' CB \u0627\u0646\u0641\u062a\u062d','Circuit Breaker');\n" +
        "    if(p.circuit==='OPEN'&&n.circuit==='CLOSED')pushEvent('Node'+n.id+' CB \u0627\u063a\u0644\u0642','Recovery');\n" +
        "  });\n" +
        "  const mc=Math.max(...d.nodes.map(n=>n.committed)),pc=Math.max(...prev.nodes.map(n=>n.committed));\n" +
        "  if(mc>pc)pushEvent('\u0627\u0644\u062a\u0632\u0627\u0645 (committed='+mc+')','Quorum');\n" +
        "  const isoNow=d.nodes.some(n=>n.isolated),isoBefore=prev.nodes.some(n=>n.isolated);\n" +
        "  if(isoNow&&!isoBefore)pushEvent('\u0627\u0646\u0642\u0633\u0627\u0645 \u0634\u0628\u0643\u0629!','CAP C>A');\n" +
        "  if(!isoNow&&isoBefore)pushEvent('\u0627\u0644\u0634\u0628\u0643\u0629 \u0627\u0646\u062f\u0645\u062c\u062a','CAP Recovery');\n" +
        "  if(d.algo!==prev.algo)pushEvent('LB: '+ALGO[d.algo],'Load Balancing');\n" +
        "  prev=d;\n" +
        "}\n" +
        "function killNode(id){act('kill',id);}\n" +
        "function reviveNode(id){act('revive',id);}\n" +
        "async function refresh(){\n" +
        "  try{\n" +
        "    const d=await(await fetch('/status')).json();\n" +
        "    detect(d);\n" +
        "    const leader=topLeader(d.nodes);\n" +
        "    document.getElementById('nodes').innerHTML=d.nodes.map(n=>{\n" +
        "      const cls=!n.alive?'down':n.isolated?'iso':n.role==='LEADER'?'leader':'';\n" +
        "      const badge=!n.alive?'<span class=\"badge b-down\">DOWN</span>':n.role==='LEADER'?'<span class=\"badge b-leader\">LEADER</span>':'<span class=\"badge b-follower\">FOLLOWER</span>';\n" +
        "      const gc='b-'+n.gossip.toLowerCase();\n" +
        "      const cbC=n.circuit==='OPEN'?'#f85149':n.circuit==='HALF_OPEN'?'#d29922':'#3fb950';\n" +
        "      const btn=n.alive?`<button class=\"danger\" onclick=\"killNode(${n.id})\">\u0627\u0642\u062a\u0644</button>`:`<button class=\"success\" onclick=\"reviveNode(${n.id})\">\u0627\u062d\u064a\u0650</button>`;\n" +
        "      return `<div class=\"card ${cls}\"><h2>Node ${n.id} ${badge} <span class=\"badge ${gc}\">${n.gossip}</span></h2>`+\n" +
        "        `<div class=\"row\"><span>term</span><b>${n.term}</b></div>`+\n" +
        "        `<div class=\"row\"><span>log/committed</span><b>${n.log}/${n.committed}</b></div>`+\n" +
        "        `<div class=\"row\"><span>avgMs / weight</span><b>${n.avgMs}ms / ${n.weight}</b></div>`+\n" +
        "        `<div class=\"row\"><span>Circuit Breaker</span><b style=\"color:${cbC}\">${n.circuit}</b></div>`+\n" +
        "        `<div style=\"margin-top:8px\">${btn}</div></div>`;\n" +
        "    }).join('');\n" +
        "    document.getElementById('algo-label').textContent='\u25cf '+ALGO[d.algo];\n" +
        "    const al2=document.getElementById('algo-label2');if(al2)al2.textContent=ALGO[d.algo];\n" +
        "    document.querySelectorAll('.algo-btn').forEach(b=>{const m=b.getAttribute('onclick').match(/'([^']+)'/);if(m)b.classList.toggle('active',m[1]===d.algo);});\n" +
        "    const ms=Math.max(1,...d.nodes.map(n=>n.served));\n" +
        "    document.getElementById('lb-bars').innerHTML=d.nodes.map(n=>\n" +
        "      `<div class=\"bar\"><span class=\"lbl\">Node ${n.id}</span>`+\n" +
        "      `<div class=\"track\"><div class=\"fill fill-blue\" style=\"width:${Math.round(n.served/ms*100)}%\"></div></div>`+\n" +
        "      `<span style=\"width:60px\">${n.served} req</span>`+\n" +
        "      `<div class=\"track\"><div class=\"fill fill-green\" style=\"width:${Math.min(100,Math.round(parseFloat(n.avgMs)*10))}%\"></div></div>`+\n" +
        "      `<span style=\"width:55px\">${n.avgMs}ms</span></div>`).join('');\n" +
        "    const kv=leader?leader.kv:{};\n" +
        "    document.getElementById('kv').innerHTML=Object.keys(kv).length?Object.entries(kv).map(([k,v])=>'<span style=\"color:#58a6ff\">'+k+'</span> = '+v).join('<br>'):'<span style=\"color:#8b949e\">(\u0641\u0627\u0631\u063a)</span>';\n" +
        "    const bh=d.bulkheads;\n" +
        "    document.getElementById('bulkheads').innerHTML=\n" +
        "      `<div class=\"bh-row\"><span class=\"bh-name\">Chat</span><span>\u0646\u0634\u0637: ${bh.chat.active}</span><span style=\"color:#f85149\">\u0645\u0631\u0641\u0648\u0636: ${bh.chat.rejected}</span></div>`+\n" +
        "      `<div class=\"bh-row\"><span class=\"bh-name\">KV</span><span>\u0646\u0634\u0637: ${bh.kv.active}</span><span style=\"color:#f85149\">\u0645\u0631\u0641\u0648\u0636: ${bh.kv.rejected}</span></div>`;\n" +
        "    document.getElementById('gossip-members').innerHTML=d.nodes.map(n=>{\n" +
        "      const c=n.gossip==='ALIVE'?'#3fb950':n.gossip==='SUSPECT'?'#d29922':'#f85149';\n" +
        "      return `<span style=\"color:${c};margin-left:10px;font-size:12px\">&#x25CF; Node${n.id}:${n.gossip}</span>`;\n" +
        "    }).join('');\n" +
        "    document.getElementById('gossip-log').innerHTML=d.gossipLogs.map(l=>'<div>'+l+'</div>').join('')||'<span>\u0644\u0627 \u0627\u062d\u062f\u0627\u062b</span>';\n" +
        "    renderRaft(d.nodes);\n" +
        "  }catch(e){}\n" +
        "}\n" +
        "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550 RAFT VISUALIZER \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n" +
        "function renderRaft(nodes){\n" +
        "  const pos={1:[250,90],2:[80,370],3:[420,370]};\n" +
        "  const leader=nodes.find(n=>n.role==='LEADER'&&n.alive);\n" +
        "  let svg='';\n" +
        "  [[1,2],[1,3],[2,3]].forEach(([a,b])=>{\n" +
        "    const [ax,ay]=pos[a],[bx,by]=pos[b];\n" +
        "    const active=leader&&(leader.id===a||leader.id===b);\n" +
        "    svg+=`<line x1=\"${ax}\" y1=\"${ay}\" x2=\"${bx}\" y2=\"${by}\" stroke=\"${active?'#58a6ff':'#4a5568'}\" stroke-width=\"${active?2.5:1.5}\" stroke-dasharray=\"${active?'10 5':'none'}\" opacity=\"${active?0.9:0.6}\"/>`;\n" +
        "  });\n" +
        "  if(leader){\n" +
        "    nodes.filter(n=>n.id!==leader.id&&n.alive).forEach(f=>{\n" +
        "      const [lx,ly]=pos[leader.id],[fx,fy]=pos[f.id];\n" +
        "      [0,0.3,0.6].forEach(d=>{\n" +
        "        svg+=`<circle r=\"4\" fill=\"#58a6ff\"><animate attributeName=\"cx\" from=\"${lx}\" to=\"${fx}\" dur=\"0.9s\" begin=\"${d}s\" repeatCount=\"indefinite\"/><animate attributeName=\"cy\" from=\"${ly}\" to=\"${fy}\" dur=\"0.9s\" begin=\"${d}s\" repeatCount=\"indefinite\"/><animate attributeName=\"opacity\" values=\"0.9;0\" dur=\"0.9s\" begin=\"${d}s\" repeatCount=\"indefinite\"/></circle>`;\n" +
        "      });\n" +
        "    });\n" +
        "  }\n" +
        "  nodes.forEach(n=>{\n" +
        "    const [x,y]=pos[n.id];\n" +
        "    const color=!n.alive?'#f85149':n.role==='LEADER'?'#58a6ff':n.role==='CANDIDATE'?'#d29922':'#8b949e';\n" +
        "    const isLeader=n.alive&&n.role==='LEADER';\n" +
        "    if(isLeader){\n" +
        "      svg+=`<circle cx=\"${x}\" cy=\"${y}\" r=\"52\" fill=\"none\" stroke=\"#58a6ff\" stroke-width=\"2\"><animate attributeName=\"r\" from=\"52\" to=\"82\" dur=\"1.4s\" repeatCount=\"indefinite\"/><animate attributeName=\"opacity\" from=\"0.6\" to=\"0\" dur=\"1.4s\" repeatCount=\"indefinite\"/></circle>`;\n" +
        "      svg+=`<circle cx=\"${x}\" cy=\"${y}\" r=\"52\" fill=\"none\" stroke=\"#58a6ff\" stroke-width=\"1.5\"><animate attributeName=\"r\" from=\"52\" to=\"82\" dur=\"1.4s\" begin=\"0.7s\" repeatCount=\"indefinite\"/><animate attributeName=\"opacity\" from=\"0.4\" to=\"0\" dur=\"1.4s\" begin=\"0.7s\" repeatCount=\"indefinite\"/></circle>`;\n" +
        "    }\n" +
        "    svg+=`<circle cx=\"${x}\" cy=\"${y}\" r=\"50\" fill=\"${n.alive?'#161b22':'#0a0d12'}\" stroke=\"${color}\" stroke-width=\"${isLeader?3:1.5}\"/>`;\n" +
        "    if(!n.alive){svg+=`<line x1=\"${x-22}\" y1=\"${y-22}\" x2=\"${x+22}\" y2=\"${y+22}\" stroke=\"#f85149\" stroke-width=\"3\" stroke-linecap=\"round\"/><line x1=\"${x+22}\" y1=\"${y-22}\" x2=\"${x-22}\" y2=\"${y+22}\" stroke=\"#f85149\" stroke-width=\"3\" stroke-linecap=\"round\"/>`;}\n" +
        "    svg+=`<text x=\"${x}\" y=\"${y-12}\" text-anchor=\"middle\" fill=\"${color}\" font-size=\"20\" font-weight=\"700\" font-family=\"system-ui\">N${n.id}</text>`;\n" +
        "    svg+=`<text x=\"${x}\" y=\"${y+8}\" text-anchor=\"middle\" fill=\"${color}\" font-size=\"11\" font-family=\"system-ui\">${!n.alive?'DEAD':n.role}</text>`;\n" +
        "    svg+=`<text x=\"${x}\" y=\"${y+24}\" text-anchor=\"middle\" fill=\"#8b949e\" font-size=\"10\" font-family=\"system-ui\">term ${n.term}</text>`;\n" +
        "    const maxB=96,bw=Math.min(n.log*6,maxB),cw=Math.min(n.committed*6,maxB);\n" +
        "    svg+=`<rect x=\"${x-48}\" y=\"${y+33}\" width=\"${maxB}\" height=\"6\" rx=\"3\" fill=\"#21262d\"/>`;\n" +
        "    svg+=`<rect x=\"${x-48}\" y=\"${y+33}\" width=\"${bw}\" height=\"6\" rx=\"3\" fill=\"${color}33\"/>`;\n" +
        "    svg+=`<rect x=\"${x-48}\" y=\"${y+33}\" width=\"${cw}\" height=\"6\" rx=\"3\" fill=\"${color}\"/>`;\n" +
        "    svg+=`<text x=\"${x}\" y=\"${y+50}\" text-anchor=\"middle\" fill=\"#8b949e66\" font-size=\"9\" font-family=\"monospace\">log=${n.log} &#x2713;=${n.committed}</text>`;\n" +
        "  });\n" +
        "  document.getElementById('raft-svg').innerHTML=svg;\n" +
        "  const ldr=nodes.find(n=>n.role==='LEADER'&&n.alive);\n" +
        "  document.getElementById('raft-info').innerHTML=ldr\n" +
        "    ?`<span style=\"color:#3fb950\">&#x2713; \u0627\u0644\u0642\u0627\u0626\u062f: <b style=\"color:#58a6ff\">Node${ldr.id}</b></span> &nbsp;|&nbsp; term: <b>${ldr.term}</b> &nbsp;|&nbsp; committed: <b>${ldr.committed}</b> &nbsp;|&nbsp; <span style=\"color:#8b949e;font-size:12px\">&#x2192; heartbeat \u0643\u0644 50ms</span>`\n" +
        "    :'<span style=\"color:#d29922\">&#x26A1; \u062c\u0627\u0631\u064d \u0627\u0646\u062a\u062e\u0627\u0628 \u0642\u0627\u0626\u062f... (Election Timeout)</span>';\n" +
        "  document.getElementById('raft-log').innerHTML=nodes.map(n=>{\n" +
        "    const c=!n.alive?'#f85149':n.role==='LEADER'?'#58a6ff':'#8b949e';\n" +
        "    return `<div class=\"raft-log-row\"><span style=\"color:${c}\">Node${n.id}</span><span style=\"color:${c}\">${!n.alive?'DEAD':n.role}</span><span>t=${n.term}</span><span>&#x2713;${n.committed}</span></div>`;\n" +
        "  }).join('');\n" +
        "}\n" +
        "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550 METRICS \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n" +
        "async function refreshMetrics(){\n" +
        "  try{\n" +
        "    const d=await(await fetch('/metrics')).json();\n" +
        "    document.getElementById('metric-chips').innerHTML=\n" +
        "      `<div class=\"m-chip\">RPS <b style=\"color:#58a6ff\">${d.rps}</b></div>`+\n" +
        "      `<div class=\"m-chip\">p50 <b>${d.p50}ms</b></div>`+\n" +
        "      `<div class=\"m-chip\">p95 <b>${d.p95}ms</b></div>`+\n" +
        "      `<div class=\"m-chip\">p99 <b style=\"color:${d.p99>50?'#f85149':'#3fb950'}\">${d.p99}ms</b></div>`+\n" +
        "      `<div class=\"m-chip\">avg <b>${d.avg}ms</b></div>`+\n" +
        "      `<div class=\"m-chip\">total <b>${d.total}</b></div>`;\n" +
        "    if(d.history&&d.history.length>=2)drawMetricsChart(d.history);\n" +
        "  }catch(e){}\n" +
        "}\n" +
        "function drawMetricsChart(hist){\n" +
        "  const canvas=document.getElementById('metrics-canvas');\n" +
        "  canvas.width=canvas.offsetWidth||canvas.parentElement.offsetWidth||900;\n" +
        "  const W=canvas.width,H=180,pad=22;\n" +
        "  const ctx=canvas.getContext('2d');\n" +
        "  ctx.clearRect(0,0,W,H);\n" +
        "  ctx.fillStyle='#0d1117';ctx.fillRect(0,0,W,H);\n" +
        "  ctx.strokeStyle='#21262d';ctx.lineWidth=1;\n" +
        "  [0.25,0.5,0.75].forEach(r=>{const y=pad+(H-2*pad)*(1-r);ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(W,y);ctx.stroke();});\n" +
        "  const maxRps=Math.max(...hist.map(h=>h.rps),1);\n" +
        "  const maxAvg=Math.max(...hist.map(h=>h.avg),1);\n" +
        "  function drawLine(vals,mx,color){\n" +
        "    ctx.save();ctx.strokeStyle=color;ctx.lineWidth=2.5;ctx.shadowColor=color;ctx.shadowBlur=8;\n" +
        "    ctx.beginPath();\n" +
        "    vals.forEach((v,i)=>{const x=(i/(vals.length-1))*(W-20)+10,y=pad+((mx-v)/mx)*(H-2*pad);i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);});\n" +
        "    ctx.stroke();ctx.restore();\n" +
        "  }\n" +
        "  drawLine(hist.map(h=>h.rps),maxRps,'#58a6ff');\n" +
        "  drawLine(hist.map(h=>h.avg),maxAvg,'#3fb950');\n" +
        "  ctx.fillStyle='#8b949e88';ctx.font='10px system-ui';ctx.textAlign='right';\n" +
        "  ctx.fillText(maxRps+' rps',W-4,pad+4);ctx.fillText(maxAvg+'ms',W-4,H-pad+2);\n" +
        "}\n" +
        "function burstLoad(){for(let i=0;i<5;i++)setTimeout(()=>act('loadtest'),i*400);}\n" +
        "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550 CHAOS \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n" +
        "let chaosActive=false,chaosTimer=null;\n" +
        "function toggleChaos(){\n" +
        "  chaosActive=!chaosActive;\n" +
        "  const btn=document.getElementById('chaos-btn');\n" +
        "  btn.textContent=chaosActive?'&#x1F6D1; \u0625\u064a\u0642\u0627\u0641 \u0627\u0644\u0641\u0648\u0636\u0649':'&#x1F534; Chaos';\n" +
        "  btn.className=chaosActive?'danger':'';\n" +
        "  if(chaosActive){pushEvent('Chaos Engineering \u0628\u062f\u0623','Chaos');runChaosStep();}\n" +
        "  else{clearTimeout(chaosTimer);act('heal');[1,2,3].forEach(i=>{act('revive',i);act('recover',i);});pushEvent('Chaos \u062a\u0648\u0642\u0641 \u2014 \u0627\u0644\u0646\u0638\u0627\u0645 \u064a\u062a\u0639\u0627\u0641\u0649','Self-Healing');}\n" +
        "}\n" +
        "function runChaosStep(){\n" +
        "  if(!chaosActive)return;\n" +
        "  const s=Math.floor(Math.random()*3);\n" +
        "  if(s===0){const id=Math.floor(Math.random()*3)+1;act('kill',id);pushEvent('Chaos: Node'+id+' \u0633\u0642\u0637 (Raft \u064a\u062a\u0639\u0627\u0645\u0644)','Fault Tolerance');setTimeout(()=>{act('revive',id);pushEvent('Chaos: Node'+id+' \u0623\u064f\u062d\u064a\u064a','Self-Healing');},2800);}\n" +
        "  else if(s===1){act('partition');pushEvent('Chaos: Network Partition (CAP Theorem)','CAP C>A');setTimeout(()=>{act('heal');pushEvent('Chaos: \u0634\u0628\u0643\u0629 \u062a\u0639\u0627\u0641\u062a','CAP Recovery');},3200);}\n" +
        "  else{const id=Math.floor(Math.random()*3)+1;act('fail',id);pushEvent('Chaos: CB Node'+id+' OPEN','Circuit Breaker');setTimeout(()=>{act('recover',id);pushEvent('Chaos: CB Node'+id+' CLOSED','Recovery');},2200);}\n" +
        "  chaosTimer=setTimeout(runChaosStep,5500);\n" +
        "}\n" +
        "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550 PACELC \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n" +
        "async function runPACELC(){\n" +
        "  const btn=document.getElementById('pacelc-btn');\n" +
        "  btn.textContent='&#x23F3; \u064a\u0642\u064a\u0633...';btn.disabled=true;\n" +
        "  const res=document.getElementById('pacelc-result');\n" +
        "  res.innerHTML='<div style=\"color:#d29922;font-size:12px;padding:8px\">&#x26A1; \u0645\u0631\u062d\u0644\u0629 1: \u0642\u064a\u0627\u0633 Baseline latency...</div>';\n" +
        "  await fetch('/action?cmd=loadtest');\n" +
        "  await sleep(1800);\n" +
        "  const d1=await(await fetch('/status')).json();\n" +
        "  const base=d1.nodes.reduce((s,n)=>s+parseFloat(n.avgMs),0)/3;\n" +
        "  res.innerHTML+='<div style=\"color:#8b949e;font-size:12px;padding:4px 8px\">&#x26A1; \u0645\u0631\u062d\u0644\u0629 2: \u062a\u0637\u0628\u064a\u0642 Network Partition...</div>';\n" +
        "  await fetch('/action?cmd=partition');\n" +
        "  pushEvent('PACELC: Partition \u0628\u062f\u0623 \u2014 \u0642\u064a\u0627\u0633 \u0627\u0644\u062a\u0623\u062b\u064a\u0631','PACELC');\n" +
        "  await sleep(500);\n" +
        "  await fetch('/action?cmd=loadtest');\n" +
        "  await sleep(1800);\n" +
        "  const d2=await(await fetch('/status')).json();\n" +
        "  const alive=d2.nodes.filter(n=>n.alive);\n" +
        "  const partLat=alive.length?alive.reduce((s,n)=>s+parseFloat(n.avgMs),0)/alive.length:9999;\n" +
        "  await fetch('/action?cmd=heal');\n" +
        "  pushEvent('PACELC: Heal \u2014 \u0627\u0633\u062a\u064f\u0639\u064a\u062f \u0627\u0644\u0627\u062a\u0633\u0627\u0642','CAP Recovery');\n" +
        "  const ratio=(partLat/Math.max(base,0.1)).toFixed(1);\n" +
        "  const cp=partLat>base*1.15;\n" +
        "  res.innerHTML=\n" +
        "    `<div style=\"background:#161b22;border:1px solid ${cp?'#58a6ff':'#d29922'};border-radius:10px;padding:14px;margin-top:8px;font-size:13px;line-height:2.1\">`+\n" +
        "    `<b style=\"color:#58a6ff;font-size:15px\">&#x1F4CB; \u0646\u062a\u064a\u062c\u0629 PACELC Proof</b><br>`+\n" +
        "    `Baseline (\u0628\u062f\u0648\u0646 partition): <b>${base.toFixed(1)}ms</b><br>`+\n" +
        "    `\u0623\u062b\u0646\u0627\u0621 Network Partition: <b style=\"color:#f85149\">${partLat.toFixed(1)}ms</b> (&#xD7;${ratio})<br>`+\n" +
        "    (cp?`<b style=\"color:#3fb950\">&#x2713; \u0627\u0644\u0646\u0638\u0627\u0645 CP \u2014 Consistency &gt; Availability</b>`:`<b style=\"color:#d29922\">&#x26A0; \u0627\u0644\u0640 latency \u0645\u062a\u0642\u0627\u0631\u0628 \u2014 \u062c\u0631\u0651\u0628 partition \u0623\u0637\u0648\u0644</b>`)+`<br>`+\n" +
        "    `<span style=\"color:#8b949e;font-size:11px\">PACELC: \u0639\u0646\u062f Partition &#x2192; \u0627\u0644\u0646\u0638\u0627\u0645 \u0631\u0641\u0639 \u0627\u0644\u0640 latency \u0628\u062f\u0644 \u0625\u0631\u062c\u0627\u0639 \u0628\u064a\u0627\u0646\u0627\u062a \u0642\u062f\u064a\u0645\u0629 &#x2192; \u064a\u064f\u062b\u0628\u062a \u0623\u0646\u0647 CP \u0645\u062b\u0644 etcd/ZooKeeper</span>`+\n" +
        "    `</div>`;\n" +
        "  btn.textContent='&#x1F4CB; PACELC Proof';btn.disabled=false;\n" +
        "}\n" +
        "function sleep(ms){return new Promise(r=>setTimeout(r,ms));}\n" +
        "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550 CHAT \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n" +
        "const CHAT='';\n" +
        "let chatUser=null,chatTo=null,lastMsgId=null;\n" +
        "function connectUser(){\n" +
        "  const u=document.getElementById('my-user').value.trim();\n" +
        "  if(!u){alert('\u0623\u062f\u062e\u0644 \u0627\u0633\u0645\u0627\u064b');return;}\n" +
        "  chatUser=u;\n" +
        "  const es=new EventSource(CHAT+'/chat/connect?user='+encodeURIComponent(u));\n" +
        "  es.onmessage=ev=>{try{const m=JSON.parse(ev.data);if(m.type==='read_receipt'){markMsgRead(m.msgId);return;}if(m.id)addMsgToUI(m);}catch(e){}};\n" +
        "  es.onerror=()=>{};\n" +
        "  alert('\u0645\u062a\u0635\u0644 \u0643\u0640 '+u+' &#x2713;');\n" +
        "}\n" +
        "function addMsgToUI(m){\n" +
        "  const box=document.getElementById('chat-msgs');\n" +
        "  const isSent=m.sender===chatUser;\n" +
        "  const vcStr=m.vc?'VC:['+m.vc.join(',')+']':'';\n" +
        "  const div=document.createElement('div');\n" +
        "  div.className='msg '+(isSent?'sent':'recv');\n" +
        "  div.id='msg-'+m.id;\n" +
        "  div.innerHTML='<div>'+m.content+'</div><div class=\"msg-meta\"><b>'+(isSent?'\u0623\u0646\u062a':m.sender)+'</b><span class=\"status-tag\">&#x2713;</span><span class=\"vc-tag\">'+vcStr+'</span><span>'+new Date(m.timestamp).toLocaleTimeString()+'</span></div>';\n" +
        "  box.appendChild(div);box.scrollTop=box.scrollHeight;lastMsgId=m.id;\n" +
        "}\n" +
        "function markMsgRead(id){const el=document.getElementById('msg-'+id);if(el){const s=el.querySelector('.status-tag');if(s){s.className='status-tag status-read';s.textContent='&#x2713;&#x2713;';}}}\n" +
        "function selectUser(u){chatTo=u;document.getElementById('chat-to-label').textContent='\u0645\u062d\u0627\u062f\u062b\u0629 \u0645\u0639 '+u;document.querySelectorAll('.user-item').forEach(i=>i.classList.toggle('selected',i.dataset.u===u));}\n" +
        "async function sendMsg(){\n" +
        "  if(!chatUser){alert('\u0627\u062a\u0635\u0644 \u0623\u0648\u0644\u0627\u064b');return;}\n" +
        "  const txt=document.getElementById('msg-input').value.trim();if(!txt)return;\n" +
        "  document.getElementById('msg-input').value='';\n" +
        "  const body='sender='+encodeURIComponent(chatUser)+'&receiver='+encodeURIComponent(chatTo||'')+'&content='+encodeURIComponent(txt);\n" +
        "  const r=await fetch(CHAT+'/chat/send',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body});\n" +
        "  const jj=await r.json();\n" +
        "  addMsgToUI({id:jj.id,sender:chatUser,receiver:chatTo,content:txt,timestamp:Date.now(),status:'SENT',vc:Array.isArray(jj.vc)?jj.vc:[]});\n" +
        "}\n" +
        "async function markRead(){if(!chatUser||!lastMsgId)return;await fetch(CHAT+'/chat/read?user='+encodeURIComponent(chatUser)+'&msgId='+encodeURIComponent(lastMsgId));markMsgRead(lastMsgId);}\n" +
        "async function setFanout(mode){await fetch(CHAT+'/chat/fanout?mode='+mode);document.getElementById('fo-write').classList.toggle('active',mode==='WRITE_TIME');document.getElementById('fo-read').classList.toggle('active',mode==='READ_TIME');}\n" +
        "async function refreshChat(){\n" +
        "  try{\n" +
        "    const d=await(await fetch(CHAT+'/chat/status')).json();\n" +
        "    const s=d.stats;\n" +
        "    document.getElementById('chat-stats').innerHTML=\n" +
        "      `<div class=\"stat-chip\">\u0645\u062a\u0635\u0644: <b>${d.online}</b></div>`+\n" +
        "      `<div class=\"stat-chip\">\u0645\u064f\u0631\u0633\u0644: <b>${s.sent}</b></div>`+\n" +
        "      `<div class=\"stat-chip\">\u0645\u064f\u0633\u0644\u064e\u0651\u0645: <b>${s.delivered}</b></div>`+\n" +
        "      `<div class=\"stat-chip\">\u0645\u0642\u0631\u0648\u0621: <b>${s.read}</b></div>`+\n" +
        "      `<div class=\"stat-chip\">\u0645\u0631\u0641\u0648\u0636: <b style=\"color:#f85149\">${s.dropped}</b></div>`+\n" +
        "      `<div class=\"stat-chip\">Fan-out: <b style=\"color:#58a6ff\">${s.fanout}</b></div>`;\n" +
        "    document.getElementById('user-list').innerHTML=d.users.map(u=>\n" +
        "      `<div class=\"user-item\" data-u=\"${u.name}\" onclick=\"selectUser(this.dataset.u)\">`+\n" +
        "      `<div class=\"dot ${u.online?'on':'off'}\"></div>${u.name}`+\n" +
        "      (u.offlineQ>0?`<span class=\"offline-badge\">${u.offlineQ}</span>`:'')+\n" +
        "      `</div>`).join('')||'<div style=\"color:#8b949e;font-size:12px\">\u0644\u0627 \u0645\u0633\u062a\u062e\u062f\u0645\u064a\u0646</div>';\n" +
        "    if(s.fanout){document.getElementById('fo-write').classList.toggle('active',s.fanout==='WRITE_TIME');document.getElementById('fo-read').classList.toggle('active',s.fanout==='READ_TIME');}\n" +
        "  }catch(e){}\n" +
        "}\n" +
        "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550 SHARED \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n" +
        "function setAlgo(name){act('algo',null,{name});}\n" +
        "function setWeight(){const id=document.getElementById('wid').value,w=document.getElementById('wval').value;if(id&&w)fetch('/action?cmd=weight&id='+id+'&w='+w);}\n" +
        "function act(cmd,id,extra){let url='/action?cmd='+cmd;if(id)url+='&id='+id;if(extra)for(const[k,v]of Object.entries(extra))url+='&'+k+'='+encodeURIComponent(v);fetch(url).then(()=>setTimeout(refresh,300));}\n" +
        "function doPut(){const k=document.getElementById('k').value,v=document.getElementById('v').value;if(k&&v)fetch('/action?cmd=put&k='+encodeURIComponent(k)+'&v='+encodeURIComponent(v)).then(()=>setTimeout(refresh,400));}\n" +
        "renderFeed();\n" +
        "setInterval(refresh,1000);\n" +
        "setInterval(refreshMetrics,2000);\n" +
        "setInterval(refreshChat,2000);\n" +
        "refresh();refreshMetrics();refreshChat();\n" +
        "</script></body></html>\n";

}
