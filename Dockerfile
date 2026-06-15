# ── مرحلة البناء ──────────────────────────────────────────────
FROM eclipse-temurin:17-jdk AS build
WORKDIR /app
COPY src ./src
# ترجمة كل ملفات الجافا
RUN javac -encoding UTF-8 -d out $(find src -name "*.java")
# نسخ واجهة لوحة التحكّم إلى جذر الـ classpath
RUN cp src/nexus/dashboard.html out/dashboard.html

# ── مرحلة التشغيل ─────────────────────────────────────────────
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/out ./out
# بورت الويب — Render يحقن متغيّر PORT تلقائياً
ENV PORT=8080
EXPOSE 8080
# -Djava.rmi.server.hostname=127.0.0.1 يضمن عمل RMI داخل الحاوية
# -XX:MaxRAMPercentage=70 يحد استهلاك الذاكرة لمناسبة النسخ المجانية (512MB)
CMD ["sh","-c","java -XX:MaxRAMPercentage=70 -Djava.rmi.server.hostname=127.0.0.1 -cp out nexus.RaftCluster"]
