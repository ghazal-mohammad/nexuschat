---
title: NexusChat
emoji: 🛰️
colorFrom: indigo
colorTo: blue
sdk: docker
app_port: 8080
pinned: false
---

# NexusChat — Distributed Systems, Live

A Java RMI platform that demonstrates distributed-systems algorithms interactively, with a web control center that shows the cluster's behavior in real time. Built for the Distributed Systems course, Damascus University.

**Ghazal Mohammad** · [Portfolio](https://ghazal-mohammad.github.io/portfolio/) · [LinkedIn](https://www.linkedin.com/in/ghazal-mohammad-0b9067250)

## What it implements

- **Raft consensus from scratch** — leader election, log replication, majority (quorum) commit
- **Replicated key/value store** on top of Raft, with idempotent requests
- **Load balancer with 8 algorithms** — Round-Robin, Weighted RR, Least Connections, Least Response Time, Sticky Session, Consistent Hashing, Power-of-Two Choices, Join-Idle-Queue
- **Fault tolerance** — Circuit Breaker, Bulkhead, Retry with exponential backoff and jitter
- **Gossip protocol** for failure detection and membership (ALIVE / SUSPECT / DEAD)
- **Vector clocks** for causal (happened-before) ordering in chat
- **Chat platform** — Server-Sent Events, presence, write-time and read-time fan-out, offline message queue with backpressure, ✓✓ read receipts with a distributed lock
- **CAP / PACELC in practice** — latency measured before, during and after a network partition
- **Concepts lab** — six live demos of the ideas above

## Tech

Java · Java RMI · Server-Sent Events · HTML/JS dashboard · Docker

## Run locally

```bash
javac -encoding UTF-8 -d out $(find src -name "*.java")
cp src/nexus/dashboard.html out/dashboard.html
java -cp out nexus.RaftCluster
# open http://localhost:8080
```

## Deploy

The `Dockerfile` compiles the sources and runs a single HTTP server that serves both the dashboard and the chat. The web port is read from the `PORT` environment variable (default 8080), so it runs as-is on Docker hosts such as Hugging Face Spaces or Render (`render.yaml` included).

---

<div dir="rtl">

## بالعربي

منصّة Java RMI تعرض مفاهيم وخوارزميات مادة الأنظمة الموزّعة بشكل تفاعلي مباشر، مع لوحة تحكّم ويب ("مركز التحكّم") تُظهر سلوك العنقود لحظياً.

- **إجماع Raft** من الصفر: انتخاب القائد، نسخ السجل، الالتزام بالأغلبية.
- **مخزن مفاتيح/قيم منسوخ** فوق Raft مع منع تكرار الطلبات.
- **موازِن أحمال** بثماني خوارزميات.
- **تحمّل الأعطال**: Circuit Breaker وBulkhead وRetry مع Exponential Backoff وJitter.
- **بروتوكول Gossip** لكشف الأعطال وإدارة العضوية.
- **Vector Clocks** للترتيب السببي في الدردشة.
- **منصّة دردشة**: SSE، حالة الاتصال، Fan-out، طابور الرسائل دون اتصال مع Backpressure، وإيصالات القراءة ✓✓ مع قفل موزّع.
- **برهان CAP / PACELC** عملي: قياس الكمون قبل انقسام الشبكة وأثناءه وبعده.

</div>
