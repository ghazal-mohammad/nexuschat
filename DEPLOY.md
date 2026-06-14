# نشر NexusChat على Render

التطبيق صار يشتغل على **بورت واحد** (لوحة التحكّم + الدردشة معاً) ويقرأ متغيّر البيئة `PORT`
الذي يحقنه Render تلقائياً. كل اللي تحتاجه: رفع الكود على GitHub ثم ربطه بـ Render.

---

## 1) جهّز Git بهوية نظيفة (بدون أي أثر)

افتح Terminal داخل مجلد المشروع `NexusChat` ونفّذ:

```bash
git init
git add .
git config user.name  "Mohammad Ghazal"
git config user.email "mohammadghazal423@gmail.com"
git commit -m "NexusChat: distributed systems showcase"
```

> ملاحظة: المشروع ما فيه أي commits سابقة، فما في تاريخ قديم لازم تنظيفه.
> الكود والتعليقات كلها عربية تقنية — ولا إشارة لأي أداة ذكاء اصطناعي.

## 2) ارفع على GitHub

أنشئ مستودعاً فارغاً على GitHub (مثلاً `nexuschat`)، ثم:

```bash
git remote add origin https://github.com/<اسم-المستخدم>/nexuschat.git
git branch -M main
git push -u origin main
```

## 3) انشر على Render

1. سجّل دخول على https://render.com (مجاني، بحساب GitHub).
2. **New → Web Service** ثم اختر مستودع `nexuschat`.
3. Render سيكتشف ملف `render.yaml` و `Dockerfile` تلقائياً:
   - Runtime: **Docker**
   - Plan: **Free**
   - Health Check Path: `/`
4. اضغط **Create Web Service** وانتظر البناء (~2-3 دقائق).
5. بيصير عندك رابط مثل: `https://nexuschat.onrender.com` — افتحه وبتطلع لوحة التحكّم.

> الخطة المجانية تنام بعد ~15 دقيقة خمول، وأول طلب بعدها يستغرق ~30 ثانية ليصحو. هذا طبيعي.

---

## كيف يعمل النشر (للفهم)

- `Dockerfile`: يترجم كل ملفات الجافا، ينسخ `dashboard.html` إلى الـ classpath، ويشغّل `nexus.RaftCluster`.
- `RaftCluster` يقرأ `PORT` من البيئة (أو 8080 محلياً) ويشغّل خادم HTTP واحد فيه:
  - لوحة التحكّم: `/`, `/status`, `/metrics`, `/action`
  - الدردشة: `/chat/connect`, `/chat/send`, `/chat/read`, `/chat/fanout`, `/chat/status`
- عقد Raft الثلاث تتواصل عبر RMI داخلياً على `localhost:1099-1101` (داخل الحاوية).

## تشغيل محلي (IntelliJ أو سطر الأوامر)

```bash
javac -encoding UTF-8 -d out $(find src -name "*.java")
cp src/nexus/dashboard.html out/dashboard.html
java -cp out nexus.RaftCluster
# افتح http://localhost:8080
```

في IntelliJ: شغّل الكلاس `RaftCluster` مباشرة ثم افتح `http://localhost:8080`.
