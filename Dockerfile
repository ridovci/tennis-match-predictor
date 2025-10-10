# ---------- TEMEL GÖRÜNTÜ ----------
FROM python:3.12-slim

# ---------- GEREKLİ SİSTEM PAKETLERİ ----------
RUN apt-get update && apt-get install -y \
    xvfb xauth wget gnupg unzip curl \
    libnss3 libatk-bridge2.0-0 libgtk-3-0 libasound2 libx11-xcb1 \
    && rm -rf /var/lib/apt/lists/*

# ---------- ÇALIŞMA DİZİNİ ----------
WORKDIR /app

# ---------- PROJEYİ KOPYALA ----------
COPY . .

# ---------- PYTHON BAĞIMLILIKLARI ----------
RUN pip install --no-cache-dir -r requirements.txt

# ---------- PLAYWRIGHT TARAYICISI ----------
RUN playwright install --with-deps chromium

# ---------- ENVIRONMENT DEĞİŞKENLERİ ----------
ENV PLAYWRIGHT_BROWSERS_PATH=0
ENV PORT=8080

# ---------- UYGULAMAYI BAŞLAT ----------
CMD ["xvfb-run", "-a", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
