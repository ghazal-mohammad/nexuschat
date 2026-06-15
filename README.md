---
title: NexusChat
emoji: 🛰️
colorFrom: indigo
colorTo: blue
sdk: docker
app_port: 8080
pinned: false
---

# NexusChat — عرض حيّ للأنظمة الموزّعة

منصّة Java RMI تعرض مفاهيم وخوارزميات مادة الأنظمة الموزّعة بشكل تفاعلي مباشر،
مع لوحة تحكّم ويب ("مركز التحكّم") تُظهر السلوك لحظياً.

## ما الذي يطبّقه المشروع

- **إجماع Raft** من الصفر: انتخاب القائد، نسخ السجل، الالتزام بالأغلبية (quorum).
- **مخزن مفاتيح/قيم منسوخ** فوق Raft مع منع تكرار الطلبات (Idempotency).
- **موازِن أحمال** بثماني خوارزميات: Round-Robin، Weighted RR، Least Connections،
  Least Response Time، Sticky Session، Consistent Hashing، Power-of-Two، Join-Idle-Queue.
- **تحمّل الأعطال**: Circuit Breaker، Bulkhead، Retry + Exponential Backoff + Jitter.
- **Gossip Protocol** لكشف الأعطال وإدارة العضوية (ALIVE / SUSPECT / DEAD).
- **Vector Clocks** للترتيب السببي (Happened-Before) في الدردشة.
- **منصّة دردشة**: SSE، إدارة حالة الاتصال، Fan-out (Write/Read-time)،
  طابور الرسائل دون اتصال مع Backpressure، إيصالات القراءة ✓✓ مع قفل موزّع.
- **برهان CAP / PACELC** عملي: قياس الكمون قبل/أثناء/بعد انقسام الشبكة.

## التشغيل محلياً

```bash
javac -encoding UTF-8 -d out $(find src -name "*.java")
cp src/nexus/dashboard.html out/dashboard.html
java -cp out nexus.RaftCluster
# افتح http://localhost:8080
```

## النشر

الحاوية (`Dockerfile`) تترجم المصدر وتشغّل خادم HTTP واحداً يخدم لوحة التحكّم
والدردشة معاً، ويقرأ بورت الويب من متغيّر البيئة `PORT` (افتراضي 8080).
